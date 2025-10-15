package src

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

func RunImportSalesInvoiceReturnCmd(args []string) {
	fs := flag.NewFlagSet("invoice-return", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/invoice_return.xlsx", "path to xlsx file")
	dsn := fs.String("dsn", "", "mysql DSN, e.g. user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true")
	adminID := fs.Int("admin-id", 1, "createdBy admin id")
	batchSize := fs.Int("batch", 500, "batch size for inserts")
	sheetName := fs.String("sheet", "", "sheet name (optional)")
	fs.Parse(args)

	start := time.Now()
	resp := Response{Success: false}

	// --- VALIDASI FILE & DSN ---
	if *dsn == "" {
		resp.Message = "dsn is required"
		printResp(resp)
		os.Exit(1)
	}
	if _, err := os.Stat(*filePath); err != nil {
		resp.Message = fmt.Sprintf("file not found: %s", *filePath)
		printResp(resp)
		os.Exit(1)
	}

	f, err := excelize.OpenFile(*filePath)
	if err != nil {
		resp.Message = "error opening file: " + err.Error()
		printResp(resp)
		os.Exit(1)
	}
	defer f.Close()

	sheet := *sheetName
	if sheet == "" {
		sheet = f.GetSheetName(0)
		if sheet == "" {
			resp.Message = "no sheet found"
			printResp(resp)
			os.Exit(1)
		}
	}

	rows, err := f.GetRows(sheet)
	if err != nil {
		resp.Message = "error reading sheet: " + err.Error()
		printResp(resp)
		os.Exit(1)
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		resp.Message = "db open error: " + err.Error()
		printResp(resp)
		os.Exit(1)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		resp.Message = "db begin error: " + err.Error()
		printResp(resp)
		os.Exit(1)
	}

	returnCols := []string{
		"return_number", "return_date", "return_note", "return_invoice_status_id",
		"branch_id", "outlet_id", "division_id", "createdAt", "createdBy", "cash_discount", "total_return",
	}
	stbCols := []string{
		"stb_number", "stb_date", "stb_status_id", "stb_type_id", "reference_number",
		"destination_warehouse_id", "issuer_type_id", "issuer_id", "issuer",
		"destination_type_id", "destination_id", "destination",
		"is_return_invoice_sales", "createdAt", "createdBy",
	}

	batchReturnRows := [][]interface{}{}
	batchStbRows := [][]interface{}{}

	inserted := 0

	for i := 1; i < len(rows); i++ { // skip header
		cols := rows[i]
		getCol := func(idx int) *string {
			if idx < len(cols) {
				return checkIsTrueEmpty(cols[idx])
			}
			return nil
		}

		if len(cols) < 10 {
			fmt.Println("column kebih kecil dari 10")
			continue
		}

		idDatePtr := getCol(0)
		invoiceNumberPtr := getCol(1)
		if invoiceNumberPtr == nil || *invoiceNumberPtr == "" {
			fmt.Println("invoice number kosong")
			continue
		}
		invoiceNumber := *invoiceNumberPtr

		invoiceDate := parseDateForSQL(idDatePtr)
		returnNote := ""
		if ptr := getCol(2); ptr != nil {
			returnNote = *ptr
		}

		branchName := ""
		if ptr := getCol(3); ptr != nil {
			branchName = strings.TrimSpace(*ptr)
		}

		divisionName := ""
		if ptr := getCol(5); ptr != nil {
			divisionName = strings.TrimSpace(*ptr)
		}
		divisionID := 3
		if strings.EqualFold(divisionName, "Pharmacy") {
			divisionID = 1
		} else if strings.EqualFold(divisionName, "Hoslab") {
			divisionID = 2
		}

		cashDiscount := denormFloat(getCol(6))
		amount := denormFloat(getCol(7))
		returnType := strings.TrimSpace(getString(getCol(8)))

		// mapping STB type
		stbTypeID := 0
		switch returnType {
		case "RO Barang Rusak":
			stbTypeID = 2 // STBType::RETUR_OUTLET_BARANG_RUSAK
		case "RO Barang Reguler":
			stbTypeID = 12 // STBType::RETUR_OUTLET_BARANG_REGULER
		default:
			stbTypeID = 1
		}

		outletCode := strings.TrimSpace(getString(getCol(9)))

		// --- cek duplicate return ---
		var exists int
		err = tx.QueryRow("SELECT COUNT(*) FROM list_invoice_return WHERE return_number = ? LIMIT 1", invoiceNumber).Scan(&exists)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error checking duplicate: " + err.Error()
			goto FINISH
		}
		if exists > 0 {
			fmt.Println("return number sudah ada: ", invoiceNumber)
			continue
		}

		// --- lookup branch ---
		var branchID int64
		err = tx.QueryRow("SELECT branch_id, branch_name FROM list_branch WHERE branch_name = ? LIMIT 1", branchName).Scan(&branchID, &branchName)
		if err == sql.ErrNoRows {
			fmt.Println("Branch not found:", branchName)
			continue
		} else if err != nil {
			_ = tx.Rollback()
			resp.Message = "error querying branch: " + err.Error()
			goto FINISH
		}

		// --- lookup outlet ---
		var outletID int64
		var outletName string
		err = tx.QueryRow("SELECT outlet_id, outlet_name FROM list_outlet WHERE outlet_code = ? LIMIT 1", outletCode).Scan(&outletID, &outletName)
		if err == sql.ErrNoRows {
			fmt.Println("Outlet not found:", outletCode)
			continue
		} else if err != nil {
			_ = tx.Rollback()
			resp.Message = "error querying outlet: " + err.Error()
			goto FINISH
		}

		// --- lookup warehouse ---
		var warehouseID int64
		warehouseTypeId := 2
		if stbTypeID == 12 {
			warehouseTypeId = 1
		}
		err = tx.QueryRow("SELECT warehouse_id FROM list_warehouse WHERE branch_id = ? AND warehouse_type_id = ? LIMIT 1", branchID, warehouseTypeId).Scan(&warehouseID)
		if err != nil {
			fmt.Println("Warehouse not found for branch:", branchName)
			continue
		}

		now := time.Now().Format("2006-01-02 15:04:05")

		// --- build return rows ---
		returnVals := []interface{}{
			invoiceNumber, invoiceDate, returnNote, 2,
			branchID, outletID, divisionID, now, *adminID, cashDiscount, amount,
		}
		batchReturnRows = append(batchReturnRows, returnVals)

		// --- build STB rows ---
		stbVals := []interface{}{
			invoiceNumber, invoiceDate, 2, stbTypeID, invoiceNumber,
			warehouseID, 3, outletID, outletName, 1, branchID, branchName,
			1, now, *adminID,
		}
		batchStbRows = append(batchStbRows, stbVals)

		if len(batchReturnRows) >= *batchSize {
			if err := flushInvoiceReturn(tx, returnCols, batchReturnRows); err != nil {
				resp.Message = err.Error()
				_ = tx.Rollback()
				goto FINISH
			}
			if err := flushSTB(tx, stbCols, batchStbRows); err != nil {
				resp.Message = err.Error()
				_ = tx.Rollback()
				goto FINISH
			}
			inserted += len(batchReturnRows)
			batchReturnRows = [][]interface{}{}
			batchStbRows = [][]interface{}{}
		}
	}

	if len(batchReturnRows) > 0 {
		if err := flushInvoiceReturn(tx, returnCols, batchReturnRows); err != nil {
			resp.Message = err.Error()
			_ = tx.Rollback()
			goto FINISH
		}
		if err := flushSTB(tx, stbCols, batchStbRows); err != nil {
			resp.Message = err.Error()
			_ = tx.Rollback()
			goto FINISH
		}
		inserted += len(batchReturnRows)
	}

	if err := tx.Commit(); err != nil {
		resp.Message = "commit error: " + err.Error()
		goto FINISH
	}

	resp.Success = true
	resp.Message = "Import Sales Invoice Return Success"
	resp.MessageDetail = fmt.Sprintf("Total %d rows inserted. Execution time: %.4fs", inserted, time.Since(start).Seconds())

FINISH:
	printResp(resp)
	log.Printf("Import Sales Invoice Return complete: %d rows, time=%.4fs\n", inserted, time.Since(start).Seconds())
}

// flushInvoiceReturn inserts batch to list_invoice_return
func flushInvoiceReturn(tx *sql.Tx, cols []string, rows [][]interface{}) error {
	base := "INSERT INTO `list_invoice_return`"
	q, args := buildMultiInsert(base, cols, rows)
	_, err := tx.Exec(q, args...)
	if err != nil {
		return fmt.Errorf("insert invoice_return failed: %v", err)
	}
	return nil
}

// flushSTB inserts batch to list_stb
func flushSTB(tx *sql.Tx, cols []string, rows [][]interface{}) error {
	base := "INSERT INTO `list_stb`"
	q, args := buildMultiInsert(base, cols, rows)
	_, err := tx.Exec(q, args...)
	if err != nil {
		return fmt.Errorf("insert stb failed: %v", err)
	}
	return nil
}

// util print response
func printResp(resp Response) {
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
}

// helper to safely deref *string
func getString(ptr *string) string {
	if ptr == nil {
		return ""
	}
	return *ptr
}
