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

	"github.com/xuri/excelize/v2"
)

func RunImportSupplierCmd(args []string) {
	fs := flag.NewFlagSet("supplier", flag.ExitOnError)

	filePath := fs.String("file", "./master_supplier.xlsx", "path to master_supplier.xlsx")
	dsn := fs.String("dsn", "", "mysql DSN, e.g. user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true")
	adminID := fs.Int("admin-id", 1, "createdBy admin id")
	batchSize := fs.Int("batch", 500, "batch size for inserts")
	sheetName := fs.String("sheet", "Daftar Supplier", "sheet name")

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

	rows, err := f.GetRows(*sheetName)
	if err != nil {
		_ = tx.Rollback()
		resp.Message = "error reading sheet: " + err.Error()
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	type townCacheValue struct {
		Valid bool
		ID    int64
	}
	townCache := map[string]townCacheValue{}
	getTownID := func(name string) townCacheValue {
		name = strings.TrimSpace(name)
		if name == "" {
			return townCacheValue{}
		}
		if v, ok := townCache[name]; ok {
			return v
		}
		var id int64
		err := tx.QueryRow("SELECT town_id FROM list_town WHERE town_name LIKE ? LIMIT 1", "%"+name+"%").Scan(&id)
		if err == nil {
			townCache[name] = townCacheValue{Valid: true, ID: id}
			return townCache[name]
		}
		townCache[name] = townCacheValue{}
		return townCache[name]
	}

	parseBool := func(ptr *string) int {
		if ptr == nil {
			return 0
		}
		s := strings.TrimSpace(strings.ToLower(*ptr))
		if s == "" || s == "-" {
			return 0
		}
		if n, err := strconv.Atoi(s); err == nil {
			if n > 0 {
				return 1
			}
			return 0
		}
		switch s {
		case "y", "yes", "true", "ya":
			return 1
		default:
			return 0
		}
	}

	getCol := func(row []string, idx int) *string {
		if idx < len(row) {
			return checkIsTrueEmpty(row[idx])
		}
		return nil
	}

	cols := []string{
		"supplier_name", "supplier_code", "origin_name", "pic", "email",
		"origin_country", "origin_city", "phone", "fax", "lock_discount",
		"is_distributor", "show_discount_routine", "allow_b2b", "show_stock_hold",
		"need_stock_correction_approval", "need_2_pm_discount_approval", "createdAt",
		"updatedAt", "createdBy", "updatedBy", "has_npwp", "npwp_number", "has_pkp",
		"pkp_number", "town_id", "is_active", "origin_pic", "origin_email",
		"origin_phone", "origin_fax", "sipnap",
	}

	batchRows := [][]interface{}{}
	inserted := 0

	for i := 1; i < len(rows); i++ { // skip header
		row := rows[i]
		for len(row) < 25 {
			row = append(row, "")
		}

		supplierNamePtr := getCol(row, 0)
		if supplierNamePtr == nil {
			continue
		}
		supplierName := *supplierNamePtr
		supplierCode := ""
		if v := getCol(row, 1); v != nil {
			supplierCode = *v
		}

		// skip if supplier already exists (by code then name)
		if supplierCode != "" {
			var tmp int
			err := tx.QueryRow("SELECT supplier_id FROM list_supplier WHERE supplier_code = ? LIMIT 1", supplierCode).Scan(&tmp)
			if err == nil {
				continue
			}
			if err != sql.ErrNoRows {
				_ = tx.Rollback()
				resp.Message = "error checking duplicate supplier_code: " + err.Error()
				goto FINISH
			}
		}
		{
			var tmp int
			err := tx.QueryRow("SELECT supplier_id FROM list_supplier WHERE supplier_name = ? LIMIT 1", supplierName).Scan(&tmp)
			if err == nil {
				continue
			}
			if err != sql.ErrNoRows {
				_ = tx.Rollback()
				resp.Message = "error checking duplicate supplier_name: " + err.Error()
				goto FINISH
			}
		}

		originName := ""
		if v := getCol(row, 2); v != nil {
			originName = *v
		}
		pic := ""
		if v := getCol(row, 3); v != nil {
			pic = *v
		}
		email := ""
		if v := getCol(row, 4); v != nil {
			email = *v
		}
		originCountry := ""
		if v := getCol(row, 5); v != nil {
			originCountry = *v
		}
		originCity := ""
		if v := getCol(row, 6); v != nil {
			originCity = *v
		}
		phone := ""
		if v := getCol(row, 7); v != nil {
			phone = *v
		}
		fax := ""
		if v := getCol(row, 8); v != nil {
			fax = *v
		}
		lockDiscount := parseBool(getCol(row, 9))
		isDistributor := parseBool(getCol(row, 10))
		showDiscountRoutine := parseBool(getCol(row, 11))
		allowB2B := parseBool(getCol(row, 12))
		showStockHold := parseBool(getCol(row, 13))
		needStockCorrectionApproval := parseBool(getCol(row, 14))
		hasNPWP := parseBool(getCol(row, 15))
		npwpNumber := ""
		if v := getCol(row, 16); v != nil {
			npwpNumber = *v
		}
		hasPKP := parseBool(getCol(row, 17))
		pkpNumber := ""
		if v := getCol(row, 18); v != nil {
			pkpNumber = *v
		}
		var townVal interface{}
		if t := getCol(row, 19); t != nil {
			tv := getTownID(*t)
			if tv.Valid {
				townVal = tv.ID
			} else {
				townVal = nil
			}
		} else {
			townVal = nil
		}
		originPic := ""
		if v := getCol(row, 20); v != nil {
			originPic = *v
		}
		originEmail := ""
		if v := getCol(row, 21); v != nil {
			originEmail = *v
		}
		originPhone := ""
		if v := getCol(row, 22); v != nil {
			originPhone = *v
		}
		originFax := ""
		if v := getCol(row, 23); v != nil {
			originFax = *v
		}
		sipnap := ""
		if v := getCol(row, 24); v != nil {
			sipnap = *v
		}

		nowStr := time.Now().Format("2006-01-02 15:04:05")

		rowVals := []interface{}{
			supplierName,                // supplier_name
			supplierCode,                // supplier_code
			originName,                  // origin_name
			pic,                         // pic
			email,                       // email
			originCountry,               // origin_country
			originCity,                  // origin_city
			phone,                       // phone
			fax,                         // fax
			lockDiscount,                // lock_discount
			isDistributor,               // is_distributor
			showDiscountRoutine,         // show_discount_routine
			allowB2B,                    // allow_b2b
			showStockHold,               // show_stock_hold
			needStockCorrectionApproval, // need_stock_correction_approval
			0,                           // need_2_pm_discount_approval
			nowStr,                      // createdAt
			nil,                         // updatedAt
			*adminID,                    // createdBy
			nil,                         // updatedBy
			hasNPWP,                     // has_npwp
			npwpNumber,                  // npwp_number
			hasPKP,                      // has_pkp
			pkpNumber,                   // pkp_number
			townVal,                     // town_id
			1,                           // is_active
			originPic,                   // origin_pic
			originEmail,                 // origin_email
			originPhone,                 // origin_phone
			originFax,                   // origin_fax
			sipnap,                      // sipnap
		}

		batchRows = append(batchRows, rowVals)

		if len(batchRows) >= *batchSize {
			base := "INSERT INTO `list_supplier`"
			q, args := buildMultiInsert(base, cols, batchRows)
			if _, err := tx.Exec(q, args...); err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting batch to list_supplier: " + err.Error()
				goto FINISH
			}
			inserted += len(batchRows)
			batchRows = [][]interface{}{}
		}
	}

	if len(batchRows) > 0 {
		base := "INSERT INTO `list_supplier`"
		q, args := buildMultiInsert(base, cols, batchRows)
		if _, err := tx.Exec(q, args...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final batch to list_supplier: " + err.Error()
			goto FINISH
		}
		inserted += len(batchRows)
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		resp.Message = "db commit error: " + err.Error()
		goto FINISH
	}

	resp.Success = true
	resp.Message = "Import Supplier Success"
	resp.MessageDetail = fmt.Sprintf("Total %d rows inserted. Execution Time: %.4fs", inserted, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import supplier complete: %d rows, time=%.4fs\n", inserted, time.Since(start).Seconds())
}
