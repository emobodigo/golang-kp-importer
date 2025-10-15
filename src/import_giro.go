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

func RunImportGiroCmd(args []string) {
	fs := flag.NewFlagSet("giro", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/giro.xlsx", "path to xlsx file")
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

	// Batch containers
	cols := []string{
		"giro_number",
		"outlet_id",
		"giro_amount",
		"due_date",
		"status_id",
		"createdAt",
		"createdBy",
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
		// indices: 0 giro_number, 1 outlet_code, 2 giro_amount, 4 due_date, 5 giro_status
		if len(rowData) < 6 {
			fmt.Println("column lebih kecil dari 6")
			continue
		}

		giroNumberPtr := getCol(0)
		if giroNumberPtr == nil {
			break // stop if no giro number
		}

		if *giroNumberPtr == "Freetext" {
			continue
		}

		outletCodePtr := getCol(1)
		giroAmountPtr := getCol(2)
		dueDatePtr := getCol(4)
		giroStatusPtr := getCol(5)

		giroNumber := strings.TrimSpace(*giroNumberPtr)

		// Parse outlet code
		outletID := int64(0)
		if outletCodePtr != nil {
			outletID = denormInt(outletCodePtr)
		}

		// Parse giro amount
		giroAmount := denormFloat(giroAmountPtr)

		// Parse due date
		dueDate := parseDateForSQL(dueDatePtr)
		if dueDate == "" {
			dueDate = time.Now().Format("2006-01-02")
		}

		// Parse giro status
		statusID := 2 // default: cair
		if giroStatusPtr != nil {
			statusStr := strings.ToLower(strings.TrimSpace(*giroStatusPtr))
			if strings.Contains(statusStr, "belum") || strings.Contains(statusStr, "belum cair") {
				statusID = 1 // belum cair
			} else {
				statusID = 2 // cair
			}
		}

		createdAt := time.Now().Format("2006-01-02 15:04:05")

		// Prepare row values in same order as cols
		rowVals := []interface{}{
			giroNumber, // giro_number
			outletID,   // outlet_id
			giroAmount, // giro_amount
			dueDate,    // due_date
			statusID,   // status_id
			createdAt,  // createdAt
			*adminID,   // createdBy
		}
		batchRows = append(batchRows, rowVals)

		// Flush batch when size reached
		for _, v := range batchRows {
			fmt.Println(v)
		}
		if len(batchRows) >= *batchSize {
			base := "INSERT INTO `list_giro_check`"

			q, sqlArgs := buildMultiInsert(base, cols, batchRows)
			if _, err := tx.Exec(q, sqlArgs...); err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting batch to list_giro_check: " + err.Error()
				goto FINISH
			}
			insertedCount += len(batchRows)
			batchRows = [][]interface{}{}
		}
	}

	// Flush remaining
	if len(batchRows) > 0 {
		base := "INSERT INTO `list_giro_check`"
		q, sqlArgs := buildMultiInsert(base, cols, batchRows)
		if _, err := tx.Exec(q, sqlArgs...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final batch to list_giro_check: " + err.Error()
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
	resp.Message = "Import Giro Success"
	resp.MessageDetail = fmt.Sprintf("Total %d rows inserted. Execution Time: %.4fs", insertedCount, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import giro complete: %d rows, time=%.4fs\n", insertedCount, time.Since(start).Seconds())
}
