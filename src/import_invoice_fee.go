package src

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/xuri/excelize/v2"
)

func RunImportSalesInvoiceFeeCmd(args []string) {
	fs := flag.NewFlagSet("invoice-fee", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/invoice_fee.xlsx", "path to xlsx file")
	dsn := fs.String("dsn", "", "mysql DSN, e.g. user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true")
	// adminID := fs.Int("admin-id", 1, "createdBy admin id")
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

	insertCols := []string{"sales_invoice_id", "fee_type_id", "amount"}
	batchRows := [][]interface{}{}
	insertedCount := 0

	for r := 1; r < len(rows); r++ { // skip header
		cols := rows[r]
		if len(cols) < 3 {
			continue
		}

		invoiceNumber := checkIsTrueEmpty(cols[0])
		feeName := checkIsTrueEmpty(cols[1])
		feeAmount := checkIsTrueEmpty(cols[2])

		if invoiceNumber == nil || feeName == nil || feeAmount == nil {
			continue
		}

		// --- lookup invoice_id ---
		var invoiceID int64
		err = tx.QueryRow("SELECT sales_invoice_id FROM list_sales_invoice WHERE sales_invoice_number = ? LIMIT 1", *invoiceNumber).Scan(&invoiceID)
		if err == sql.ErrNoRows {
			fmt.Printf("Invoice tidak ditemukan: %s\n", *invoiceNumber)
			continue
		} else if err != nil {
			_ = tx.Rollback()
			resp.Message = "error querying invoice: " + err.Error()
			goto FINISH
		}

		// --- determine fee type ---
		var feeTypeID int64 = 1
		if *feeName == "Ongkos Kirim" {
			feeTypeID = 2
		}

		amount := denormFloat(feeAmount)

		batchRows = append(batchRows, []interface{}{
			invoiceID,
			feeTypeID,
			amount,
		})

		if len(batchRows) >= *batchSize {
			base := "INSERT INTO `rel_sales_invoice_fees`"
			q, args := buildMultiInsert(base, insertCols, batchRows)
			if _, err := tx.Exec(q, args...); err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting batch to rel_sales_invoice_fees: " + err.Error()
				goto FINISH
			}
			insertedCount += len(batchRows)
			batchRows = [][]interface{}{}
		}
	}

	// flush remainder
	if len(batchRows) > 0 {
		base := "INSERT INTO `rel_sales_invoice_fees`"
		q, args := buildMultiInsert(base, insertCols, batchRows)
		if _, err := tx.Exec(q, args...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final batch to rel_sales_invoice_fees: " + err.Error()
			goto FINISH
		}
		insertedCount += len(batchRows)
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		resp.Message = "db commit error: " + err.Error()
		goto FINISH
	}

	resp.Success = true
	resp.Message = "Import Sales Invoice Fee Success"
	resp.MessageDetail = fmt.Sprintf("Total %d rows inserted. Execution Time: %.4fs", insertedCount, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import sales invoice fee complete: %d rows, time=%.4fs\n", insertedCount, time.Since(start).Seconds())
}
