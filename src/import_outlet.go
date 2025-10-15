package src

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xuri/excelize/v2"
)

type Response struct {
	Success       bool   `json:"success"`
	Message       string `json:"message"`
	MessageDetail string `json:"message_detail"`
}

func RunImportOutletCmd(args []string) {
	fs := flag.NewFlagSet("outlet", flag.ExitOnError)

	filePath := fs.String("file", "./uploads/outlet.xlsx", "path to xlsx file")
	dsn := fs.String("dsn", "", "mysql DSN, e.g. user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true")
	adminID := fs.Int("admin-id", 1, "createdBy admin id")
	batchSize := fs.Int("batch", 500, "batch size for inserts")
	logID := fs.String("log-id", "", "optional log_id")
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

	// open excel
	f, err := excelize.OpenFile(*filePath)
	if err != nil {
		resp.Message = "error opening file: " + err.Error()
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	// choose sheet
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

	// get rows
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

	// begin transaction
	tx, err := db.Begin()
	if err != nil {
		resp.Message = "db begin error: " + err.Error()
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	// prepare batches
	batchOutletRows := [][]interface{}{}
	batchOutletCols := []string{
		"outlet_id", "outlet_name", "outlet_code", "outlet_pic", "outlet_status_id",
		"credit_limit", "top_lock", "top_value", "lock_discount", "lock_cash_discount",
		"minimum_invoice_value", "sipnap_code", "branch_id", "segment_internal_id",
		"npwp", "is_pkp", "pkp", "is_pbf", "pbf_code", "outlet_type_id",
		"show_npwp_print", "tax_document_type", "nik", "nitku", "outlet_note",
		"createdAt", "createdBy",
	}
	batchHistoryRows := [][]interface{}{}
	batchHistoryCols := []string{
		"outlet_id", "outlet_name", "outlet_code", "outlet_pic", "outlet_status_id",
		"credit_limit", "top_lock", "top_value", "lock_discount", "lock_cash_discount",
		"minimum_invoice_value", "sipnap_code", "branch_id", "segment_internal_id",
		"npwp", "is_pkp", "pkp", "is_pbf", "pbf_code", "outlet_type_id",
		"show_npwp_print", "tax_document_type", "nik", "nitku", "outlet_note",
		"createdAt", "createdBy", "history_status_id",
	}

	succeedRows := []int{}
	failedRows := []string{}
	uniqueSipnap := map[string]bool{}
	missingOutletList := map[string]bool{}

	// iterate rows starting from row 2 (index 1), as PHP did
	currentRow := 2
	for i := 1; i < len(rows); i++ {
		cols := rows[i]
		// if first column empty -> break
		var firstCol string
		if len(cols) > 0 {
			firstCol = cols[0]
		}
		if checkIsTrueEmpty(firstCol) == nil {
			continue
		}

		// map columns to variables (guarding index)
		getCol := func(idx int) *string {
			if idx < len(cols) {
				return checkIsTrueEmpty(cols[idx])
			}
			return nil
		}

		var outletIDPtr *int
		outletIDStrPtr := getCol(0)
		if outletIDStrPtr != nil && *outletIDStrPtr != "" {
			if n, err := strconv.Atoi(*outletIDStrPtr); err == nil {
				outletIDPtr = &n
			} else {
				fmt.Printf("⚠️  Row %d skipped: invalid outlet_id '%s'\n", currentRow, *outletIDStrPtr)
				currentRow++
				continue // skip baris yang id-nya tidak valid
			}
		}
		outletNamePtr := getCol(1)
		outletCodePtr := getCol(2)
		if len(missingOutletList) > 0 {
			// if not in missing list, continue (mirror PHP logic)
			if outletCodePtr != nil {
				if _, ok := missingOutletList[*outletCodePtr]; !ok {
					// skip
					currentRow++
					fmt.Println("missing outlet called")
					continue
				}
			}
		}
		outletPicPtr := getCol(3)
		creditLimit := denormalizeNumber(getCol(4))
		topLock := 0
		if getCol(5) != nil && strings.EqualFold(*getCol(5), "Ya") {
			topLock = 1
		}
		topValue := denormalizeNumber(getCol(6))
		lockDiscount := 0
		if getCol(7) != nil && strings.EqualFold(*getCol(7), "Ya") {
			lockDiscount = 0
		}
		lockCashDiscount := 0
		if getCol(8) != nil && strings.EqualFold(*getCol(8), "Ya") {
			lockCashDiscount = 1
		}
		minimumInvoiceValue := denormalizeNumber(getCol(9))
		sipnapCode := ""
		if getCol(10) != nil {
			sipnapCode = *getCol(10)
		}
		branchNameVal := ""
		if getCol(11) != nil {
			branchNameVal = *getCol(11)
		}
		segmentInternalVal := ""
		if getCol(12) != nil {
			segmentInternalVal = *getCol(12)
		}
		npwpVal := ""
		if getCol(13) != nil {
			npwpVal = *getCol(13)
		}
		pkpVal := ""
		if getCol(14) != nil {
			pkpVal = *getCol(14)
		}
		isPkp := 0
		if pkpVal != "" {
			isPkp = 1
		}
		pbfCode := ""
		if getCol(15) != nil {
			pbfCode = *getCol(15)
		}
		isPbf := 0
		if pbfCode != "" {
			isPbf = 1
		}
		outletType := 2
		if getCol(16) != nil && len(*getCol(16)) >= 2 && (*getCol(16))[:2] == "01" {
			outletType = 1
		}
		nik := ""
		if getCol(17) != nil {
			nik = *getCol(17)
		}
		nitku := ""
		if getCol(18) != nil {
			nitku = *getCol(18)
		}
		outletNote := ""
		if getCol(19) != nil {
			outletNote = *getCol(19)
		}
		outletStatus := 3
		if getCol(20) != nil && strings.EqualFold(*getCol(20), "AKTIF") {
			outletStatus = 2
		}
		taxDocumentType := "Dokumen dengan NIK"
		if npwpVal != "" {
			taxDocumentType = "Dokumen dengan NPWP/NIK tervalidasi"
		}

		// duplicate sipnap check
		if sipnapCode != "" {
			if _, exists := uniqueSipnap[sipnapCode]; exists {
				failedRows = append(failedRows, fmt.Sprintf("<b>[<span style='color: orange;'>%d</span> Duplikat Sipnap]</b>", currentRow))
				currentRow++
				continue
			}
			dup, err := checkDuplicate(tx, "list_outlet", "sipnap_code", sipnapCode)
			if err != nil {
				_ = tx.Rollback()
				resp.Message = "db error checking duplicate: " + err.Error()
				resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
				out, _ := json.Marshal(resp)
				fmt.Println(string(out))
				os.Exit(1)
			}
			if dup {
				fmt.Println("duplicate sipnap")
				failedRows = append(failedRows, fmt.Sprintf("<b>[<span style='color: orange;'>%d</span> Duplikat Sipnap]</b>", currentRow))
				currentRow++
				continue
			}
			uniqueSipnap[sipnapCode] = true
		}

		// check_import_column for branch_name and segment_internal_name
		branchID := sql.NullInt64{Valid: false}
		if branchNameVal != "" {
			bID, err := checkImportColumn(tx, "branch_name", "list_branch", branchNameVal, nil)
			if err != nil {
				_ = tx.Rollback()
				resp.Message = "error checkImportColumn(list_branch): " + err.Error()
				resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
				out, _ := json.Marshal(resp)
				fmt.Println(string(out))
				os.Exit(1)
			}
			branchID = bID
		}
		segmentInternalID := sql.NullInt64{Valid: false}
		if segmentInternalVal != "" {
			segID, err := checkImportColumn(tx, "segment_name", "list_outlet_segment", segmentInternalVal, map[string]string{"internal": "true"})
			if err != nil {
				_ = tx.Rollback()
				resp.Message = "error checkImportColumn(list_outlet_segment): " + err.Error()
				resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
				out, _ := json.Marshal(resp)
				fmt.Println(string(out))
				os.Exit(1)
			}
			segmentInternalID = segID
		}

		createdAt := time.Now().Format("2006-01-02 15:04:05")

		outletID := 0
		if outletIDPtr != nil {
			outletID = *outletIDPtr
		}
		outletName := ""
		if outletNamePtr != nil {
			outletName = *outletNamePtr
		}
		outletCode := ""
		if outletCodePtr != nil {
			outletCode = *outletCodePtr
		}
		outletPic := ""
		if outletPicPtr != nil {
			outletPic = *outletPicPtr
		}

		// Build row values in same order as batchOutletCols
		rowVals := []interface{}{
			outletID,
			outletName,
			outletCode,
			outletPic,
			outletStatus,
			creditLimit,
			topLock,
			topValue,
			lockDiscount,
			lockCashDiscount,
			minimumInvoiceValue,
			sipnapCode,
		}
		// branch_id
		if branchID.Valid {
			rowVals = append(rowVals, branchID.Int64)
		} else {
			rowVals = append(rowVals, nil)
		}
		// segment_internal_id
		if segmentInternalID.Valid {
			rowVals = append(rowVals, segmentInternalID.Int64)
		} else {
			rowVals = append(rowVals, nil)
		}
		rowVals = append(rowVals,
			npwpVal,
			isPkp,
			pkpVal,
			isPbf,
			pbfCode,
			outletType,
			1, // show_npwp_print
			taxDocumentType,
			nik,
			nitku,
			outletNote,
			createdAt,
			*adminID,
		)

		batchOutletRows = append(batchOutletRows, rowVals)

		// Prepare history row (same columns + history_status_id)
		hrow := append([]interface{}{}, rowVals...)
		hrow = append(hrow, 2) // history_status_id
		batchHistoryRows = append(batchHistoryRows, hrow)

		// if reached batch size -> flush
		if len(batchOutletRows) >= *batchSize {
			// insert into list_outlet
			base := "INSERT INTO `list_outlet`"
			q, args := buildMultiInsert(base, batchOutletCols, batchOutletRows)
			if _, err := tx.Exec(q, args...); err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting batch to list_outlet: " + err.Error()
				for _, v := range batchOutletRows {
					fmt.Println(v)
				}
				resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
				out, _ := json.Marshal(resp)
				fmt.Println(string(out))
				os.Exit(1)
			}
			// insert into list_outlet_history
			baseH := "INSERT INTO `list_outlet_history`"
			qh, argsh := buildMultiInsert(baseH, batchHistoryCols, batchHistoryRows)
			if _, err := tx.Exec(qh, argsh...); err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting batch to list_outlet_history: " + err.Error()
				resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
				out, _ := json.Marshal(resp)
				fmt.Println(string(out))
				os.Exit(1)
			}
			// clear
			batchOutletRows = [][]interface{}{}
			batchHistoryRows = [][]interface{}{}
		}

		succeedRows = append(succeedRows, currentRow)
		fmt.Println("Insert row: ", currentRow)
		currentRow++
	}

	// flush remaining batch
	if len(batchOutletRows) > 0 {
		base := "INSERT INTO `list_outlet`"
		q, args := buildMultiInsert(base, batchOutletCols, batchOutletRows)
		if _, err := tx.Exec(q, args...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final batch to list_outlet: " + err.Error()
			resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
			out, _ := json.Marshal(resp)
			fmt.Println(string(out))
			os.Exit(1)
		}
		baseH := "INSERT INTO `list_outlet_history`"
		qh, argsh := buildMultiInsert(baseH, batchHistoryCols, batchHistoryRows)
		if _, err := tx.Exec(qh, argsh...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final batch to list_outlet_history: " + err.Error()
			resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
			out, _ := json.Marshal(resp)
			fmt.Println(string(out))
			os.Exit(1)
		}
	}

	// commit transaction
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		resp.Message = "db commit error: " + err.Error()
		resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	// optional update activity if log-id provided
	if *logID != "" {
		// begin new tx for updateActivity
		tx2, err := db.Begin()
		if err == nil {
			_ = updateActivity(tx2, *logID, "IMPORT DATA OUTLET", "outlet/view_outlet_list", "{}")
			_ = tx2.Commit()
		}
	}

	// prepare response message_detail
	messageDetail := fmt.Sprintf("<p>- Total <b>%d</b> baris data berhasil disimpan</p>", len(succeedRows))
	messageDetail += fmt.Sprintf("<p>- Total <b>%d</b> baris duplikat data gagal disimpan</p>", len(failedRows))
	if len(failedRows) > 0 {
		messageDetail += "<div class='gs-grid-container column-2 scrollable-y' style='max-height: 250px;'>"
		for _, v := range failedRows {
			messageDetail += v
		}
		messageDetail += "</div>"
	}
	end := time.Now()
	messageDetail += fmt.Sprintf("Execution Time : %.4f seconds", end.Sub(start).Seconds())

	resp.Success = true
	resp.Message = "Import Outlet Success"
	resp.MessageDetail = messageDetail

	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import complete: %d rows succeeded, %d duplicates, time=%.4fs\n", len(succeedRows), len(failedRows), end.Sub(start).Seconds())
}
