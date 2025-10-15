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

func RunImportSalesInvoiceReturnProductCmd(args []string) {
	fs := flag.NewFlagSet("invoice-return-product", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/invoice_return_product.xlsx", "path to xlsx file")
	dsn := fs.String("dsn", "", "mysql DSN, e.g. user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true")
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

	// Get missing return invoice list (invoices without items)
	missingReturnInvoices := make(map[int64]bool)
	queryMissing := `
		SELECT li.return_invoice_id 
		FROM list_invoice_return li 
		LEFT JOIN rel_return_invoice_stb lpi ON li.return_invoice_id = lpi.return_invoice_id 
		WHERE lpi.return_invoice_id IS NULL
	`
	rowsMissing, err := db.Query(queryMissing)
	if err != nil {
		resp.Message = "error querying missing return invoices: " + err.Error()
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}
	for rowsMissing.Next() {
		var returnInvoiceID int64
		if err := rowsMissing.Scan(&returnInvoiceID); err == nil {
			missingReturnInvoices[returnInvoiceID] = true
		}
	}
	rowsMissing.Close()

	tx, err := db.Begin()
	if err != nil {
		resp.Message = "db begin error: " + err.Error()
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	// Caches
	returnInvoiceCache := make(map[string]*ReturnInvoiceData)
	stbCache := make(map[string]*STBData)
	productCache := make(map[string]int64)

	// Batch containers
	cols := []string{
		"return_invoice_id", "stb_id", "product_id", "unit", "qty", "qty_extra",
		"quoted_price", "discount_price", "discount_routine_branch", "discount_routine_central",
		"discount_program_branch", "discount_program_central", "discount_extra", "hna",
		"total_price", "batch_number", "serial_number", "expired_date", "reference_type_id", "reference_id",
	}
	batchRows := [][]interface{}{}
	insertedCount := 0
	rowIndex := 0

	for r := 1; r < len(rows); r++ { // skip header
		rowIndex++
		rowData := rows[r]

		getCol := func(idx int) *string {
			if idx < len(rowData) {
				return checkIsTrueEmpty(rowData[idx])
			}
			return nil
		}

		if len(rowData) < 10 {
			fmt.Println("column lebih kecil dari 10")
			continue
		}

		invoiceNumberPtr := getCol(0)
		productCodePtr := getCol(1)
		qtyPtr := getCol(2)
		qtyExtraPtr := getCol(3)
		batchNumberPtr := getCol(4)
		expiredDatePtr := getCol(6)
		pricePtr := getCol(7)
		discountRoutinePtr := getCol(8)
		discountProgramPtr := getCol(9)

		if invoiceNumberPtr == nil || productCodePtr == nil {
			fmt.Println("invoice dan product code kosong")
			continue
		}

		invoiceNumber := strings.TrimSpace(*invoiceNumberPtr)
		productCode := strings.TrimSpace(*productCodePtr)

		// Get or cache return invoice
		var returnInvoice *ReturnInvoiceData
		if cached, ok := returnInvoiceCache[invoiceNumber]; ok {
			returnInvoice = cached
		} else {
			var retID int64
			err = tx.QueryRow(`
				SELECT return_invoice_id 
				FROM list_invoice_return 
				WHERE return_number = ? 
				LIMIT 1
			`, invoiceNumber).Scan(&retID)

			if err == sql.ErrNoRows {
				fmt.Printf("Return invoice not found: %s\n", invoiceNumber)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying return invoice: " + err.Error()
				goto FINISH
			}
			returnInvoice = &ReturnInvoiceData{ReturnInvoiceID: retID}
			returnInvoiceCache[invoiceNumber] = returnInvoice
		}

		// Check if invoice is in missing list
		if !missingReturnInvoices[returnInvoice.ReturnInvoiceID] {
			fmt.Println("invoice in missing list")
			continue
		}

		// Get or cache STB
		var stb *STBData
		if cached, ok := stbCache[invoiceNumber]; ok {
			stb = cached
		} else {
			var stbID int64
			err = tx.QueryRow(`
				SELECT stb_id 
				FROM list_stb 
				WHERE stb_number = ? 
				LIMIT 1
			`, invoiceNumber).Scan(&stbID)

			if err == sql.ErrNoRows {
				fmt.Printf("STB not found: %s\n", invoiceNumber)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying stb: " + err.Error()
				goto FINISH
			}
			stb = &STBData{STBID: stbID}
			stbCache[invoiceNumber] = stb
		}

		// Get or cache product
		var productID int64
		if cached, ok := productCache[productCode]; ok {
			productID = cached
		} else {
			err = tx.QueryRow(`
				SELECT product_id 
				FROM list_product 
				WHERE product_code = ? 
				LIMIT 1
			`, productCode).Scan(&productID)

			if err == sql.ErrNoRows {
				fmt.Printf("Product not found: %s\n", productCode)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying product: " + err.Error()
				goto FINISH
			}
			productCache[productCode] = productID
		}

		qty := denormInt(qtyPtr)
		qtyExtra := denormInt(qtyExtraPtr)
		batchNumber := ""
		if batchNumberPtr != nil {
			batchNumber = strings.TrimSpace(*batchNumberPtr)
		}
		expiredDate := parseDateForSQL(expiredDatePtr)
		price := denormFloat(pricePtr)
		discountRoutine := denormFloat(discountRoutinePtr)
		discountProgram := denormFloat(discountProgramPtr)

		// Calculate values
		discountRoutineValue := discountRoutine / 100 * price
		discountProgramValue := discountProgram / 100 * price
		discountValue := discountRoutineValue + discountProgramValue

		// Main product entry
		if qty > 0 {
			hnaMain := price * float64(qty)
			discountExtra := price * float64(qtyExtra)
			totalPriceMain := hnaMain - discountExtra - discountValue

			rowVals := []interface{}{
				returnInvoice.ReturnInvoiceID, // return_invoice_id
				stb.STBID,                     // stb_id
				productID,                     // product_id
				1,                             // unit (always 1)
				qty,                           // qty
				qtyExtra,                      // qty_extra
				price,                         // quoted_price
				nil,                           // discount_price
				discountRoutine,               // discount_routine_branch
				0,                             // discount_routine_central
				discountProgram,               // discount_program_branch
				0,                             // discount_program_central
				discountExtra,                 // discount_extra
				hnaMain,                       // hna
				totalPriceMain,                // total_price
				batchNumber,                   // batch_number
				nil,                           // serial_number
				expiredDate,                   // expired_date
				1,                             // reference_type_id
				nil,                           // reference_id
			}
			batchRows = append(batchRows, rowVals)
		}

		// Flush batch when size reached
		if len(batchRows) >= *batchSize {
			base := "INSERT INTO `rel_return_invoice_stb`"
			q, sqlArgs := buildMultiInsert(base, cols, batchRows)
			if _, err := tx.Exec(q, sqlArgs...); err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting batch to rel_return_invoice_stb: " + err.Error()
				goto FINISH
			}
			insertedCount += len(batchRows)
			batchRows = [][]interface{}{}
		}
	}

	// Flush remaining
	if len(batchRows) > 0 {
		base := "INSERT INTO `rel_return_invoice_stb`"
		q, sqlArgs := buildMultiInsert(base, cols, batchRows)
		if _, err := tx.Exec(q, sqlArgs...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final batch to rel_return_invoice_stb: " + err.Error()
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
	resp.Message = "Import Sales Invoice Return Product Success"
	resp.MessageDetail = fmt.Sprintf("Total %d rows inserted. Execution Time: %.4fs", insertedCount, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import sales invoice return product complete: %d rows, time=%.4fs\n", insertedCount, time.Since(start).Seconds())
}

// Helper structs
type ReturnInvoiceData struct {
	ReturnInvoiceID int64
}

type STBData struct {
	STBID int64
}
