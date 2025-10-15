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

func RunImportDepositCmd(args []string) {
	fs := flag.NewFlagSet("deposit", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/deposit.xlsx", "path to xlsx file")
	dsn := fs.String("dsn", "", "mysql DSN, e.g. user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true")
	adminID := fs.Int("admin-id", 1, "createdBy admin id")
	batchSize := fs.Int("batch", 500, "batch size for inserts")
	sheetName := fs.String("sheet", "", "sheet name (optional)")
	fs.Parse(args)

	start := time.Now()
	resp := Response{Success: false}

	if *dsn == "" {
		resp.Message = "dsn is required"
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}
	if _, err := os.Stat(*filePath); err != nil {
		resp.Message = fmt.Sprintf("file not found: %s", *filePath)
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	f, err := excelize.OpenFile(*filePath)
	if err != nil {
		resp.Message = "error opening file: " + err.Error()
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}
	defer f.Close()

	sheet := *sheetName
	if sheet == "" {
		sheet = f.GetSheetName(0)
		if sheet == "" {
			resp.Message = "no sheet found"
			out, _ := json.Marshal(resp)
			fmt.Println(string(out))
			os.Exit(1)
		}
	}

	rows, err := f.GetRows(sheet)
	if err != nil {
		resp.Message = "error reading sheet rows: " + err.Error()
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		resp.Message = "db open error: " + err.Error()
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		resp.Message = "db begin error: " + err.Error()
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	// Caches
	branchCache := make(map[string]*BranchData)
	invoiceCache := make(map[string]*int64)
	returnInvoiceCache := make(map[string]*int64)

	// Batch containers
	cols := []string{
		"deposit_date", "deposit_number", "deposit_type_id", "outlet_id",
		"branch_id", "deposit_location_id", "debit", "credit",
		"note", "settlement_id", "sales_invoice_id", "return_invoice_id",
		"createdAt", "createdBy",
	}
	batchRows := [][]interface{}{}
	insertedCount := 0
	rowIndex := 0

	for r := 1; r < len(rows); r++ { // skip header row
		rowIndex++
		rowData := rows[r]

		getCol := func(idx int) *string {
			if idx < len(rowData) {
				return checkIsTrueEmpty(rowData[idx])
			}
			return nil
		}

		// Ensure minimum columns
		// indices: 0 date, 1 deposit_type, 2 branch_code, 3 outlet_code, 4 debit, 5 deposit_number, 6 invoice_number, 7 invoice_return_number
		if len(rowData) < 5 {
			fmt.Println("column less than 5")
			continue
		}

		datePtr := getCol(0)
		if datePtr == nil {
			break // stop if no date
		}

		depositTypePtr := getCol(1)
		branchCodePtr := getCol(2)
		if *branchCodePtr == "Freetext" {
			continue
		}
		outletCodePtr := getCol(3)
		debitPtr := getCol(4)
		depositNumberPtr := getCol(5)
		invoiceNumberPtr := getCol(6)
		invoiceReturnNumberPtr := getCol(7)

		if branchCodePtr == nil {
			fmt.Println("branch code nil")
			continue
		}

		// Parse deposit date
		depositDate := parseDateForSQL(datePtr)
		if depositDate == "" {
			depositDate = time.Now().Format("2006-01-02")
		}

		// Determine deposit type
		depositTypeID := 3
		depositNote := "DEPOSIT"
		if depositTypePtr != nil {
			depositTypeStr := strings.ToLower(strings.TrimSpace(*depositTypePtr))
			if strings.Contains(depositTypeStr, "pelunasan") {
				depositTypeID = 1
				depositNote = "DEPOSIT PELUNASAN"
			} else {
				depositTypeID = 3
				depositNote = "DEPOSIT INVOICE RETUR"
			}
		}

		branchCode := strings.TrimSpace(*branchCodePtr)

		// Get or cache branch
		var branch *BranchData
		if cached, ok := branchCache[branchCode]; ok {
			branch = cached
		} else {
			var branchID int64
			err = tx.QueryRow(`
				SELECT branch_id 
				FROM list_branch 
				WHERE branch_code = ? 
				LIMIT 1
			`, branchCode).Scan(&branchID)

			if err == sql.ErrNoRows {
				fmt.Printf("Branch not found: %s\n", branchCode)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying branch: " + err.Error()
				goto FINISH
			}
			branch = &BranchData{BranchID: branchID}
			branchCache[branchCode] = branch
		}

		// Parse outlet code
		outletID := int64(0)
		if outletCodePtr != nil {
			outletID = denormInt(outletCodePtr)
		}

		// Parse debit
		debit := denormFloat(debitPtr)

		// Handle deposit number
		depositNumber := ""
		if depositNumberPtr != nil {
			depositNumber = strings.TrimSpace(*depositNumberPtr)
		}

		// Handle sales invoice
		var salesInvoiceID *int64
		if invoiceNumberPtr != nil {
			invoiceNumber := strings.TrimSpace(*invoiceNumberPtr)
			if invoiceNumber != "" {
				if cached, ok := invoiceCache[invoiceNumber]; ok {
					salesInvoiceID = cached
				} else {
					var invID int64
					err = tx.QueryRow(`
						SELECT sales_invoice_id 
						FROM list_sales_invoice 
						WHERE sales_invoice_number = ? 
						LIMIT 1
					`, invoiceNumber).Scan(&invID)

					if err == sql.ErrNoRows {
						salesInvoiceID = nil
						invoiceCache[invoiceNumber] = nil
					} else if err != nil {
						_ = tx.Rollback()
						resp.Message = "error querying sales invoice: " + err.Error()
						goto FINISH
					} else {
						salesInvoiceID = &invID
						invoiceCache[invoiceNumber] = &invID
					}
				}
			}
		}

		// Handle return invoice
		var returnInvoiceID *int64
		if invoiceReturnNumberPtr != nil {
			returnInvoiceNumber := strings.TrimSpace(*invoiceReturnNumberPtr)
			if returnInvoiceNumber != "" {
				// If return invoice exists, use column 7 as deposit_number
				if depositNumber == "" {
					depositNumber = returnInvoiceNumber
				}

				if cached, ok := returnInvoiceCache[returnInvoiceNumber]; ok {
					returnInvoiceID = cached
				} else {
					var retInvID int64
					err = tx.QueryRow(`
						SELECT return_invoice_id 
						FROM list_invoice_return 
						WHERE return_number = ? 
						LIMIT 1
					`, returnInvoiceNumber).Scan(&retInvID)

					if err == sql.ErrNoRows {
						returnInvoiceID = nil
						returnInvoiceCache[returnInvoiceNumber] = nil
					} else if err != nil {
						_ = tx.Rollback()
						resp.Message = "error querying return invoice: " + err.Error()
						goto FINISH
					} else {
						returnInvoiceID = &retInvID
						returnInvoiceCache[returnInvoiceNumber] = &retInvID
					}
				}
			}
		}

		createdAt := time.Now().Format("2006-01-02 15:04:05")

		// Prepare row values in same order as cols
		rowVals := []interface{}{
			depositDate,     // deposit_date
			depositNumber,   // deposit_number
			depositTypeID,   // deposit_type_id
			outletID,        // outlet_id
			branch.BranchID, // branch_id
			branch.BranchID, // deposit_location_id (same as branch_id)
			debit,           // debit
			0,               // credit
			depositNote,     // note
			nil,             // settlement_id
			salesInvoiceID,  // sales_invoice_id
			returnInvoiceID, // return_invoice_id
			createdAt,       // createdAt
			*adminID,        // createdBy
		}
		batchRows = append(batchRows, rowVals)

		// Flush batch when size reached
		if len(batchRows) >= *batchSize {
			base := "INSERT INTO `list_outlet_deposit`"
			q, sqlArgs := buildMultiInsert(base, cols, batchRows)
			if _, err := tx.Exec(q, sqlArgs...); err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting batch to list_outlet_deposit: " + err.Error()
				goto FINISH
			}
			insertedCount += len(batchRows)
			batchRows = [][]interface{}{}
		}
	}

	// Flush remaining
	if len(batchRows) > 0 {
		base := "INSERT INTO `list_outlet_deposit`"
		q, sqlArgs := buildMultiInsert(base, cols, batchRows)
		if _, err := tx.Exec(q, sqlArgs...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final batch to list_outlet_deposit: " + err.Error()
			goto FINISH
		}
		insertedCount += len(batchRows)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		resp.Message = "db commit error: " + err.Error()
		goto FINISH
	}

	resp.Success = true
	resp.Message = "Import Deposit Success"
	resp.MessageDetail = fmt.Sprintf("Total %d rows inserted. Execution Time: %.4fs", insertedCount, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import deposit complete: %d rows, time=%.4fs\n", insertedCount, time.Since(start).Seconds())
}

// Helper struct
type BranchData struct {
	BranchID int64
}
