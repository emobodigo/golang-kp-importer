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

func RunImportProductCmd(args []string) {
	fs := flag.NewFlagSet("product", flag.ExitOnError)

	filePath := fs.String("file", "./uploads/product.xlsx", "path to xlsx file")
	dsn := fs.String("dsn", "", "mysql DSN, e.g. user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true")
	adminID := fs.Int("admin-id", 1, "createdBy admin id")
	batchSize := fs.Int("batch", 500, "batch size for inserts")
	logID := fs.String("log-id", "", "optional log_id")

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

	// begin transaction as in PHP
	tx, err := db.Begin()
	if err != nil {
		resp.Message = "db begin error: " + err.Error()
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	messageDetailBuilder := strings.Builder{}

	// ---- Sheet: Daftar Produk ----
	if err := importDaftarProduk(f, tx, *batchSize, *adminID, &messageDetailBuilder); err != nil {
		_ = tx.Rollback()
		resp.Message = "error importing Daftar Produk: " + err.Error()
		resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	// ---- Sheet: Zat Aktif Produk ----
	if err := importZatAktifProduk(f, tx, *batchSize, *adminID, &messageDetailBuilder); err != nil {
		_ = tx.Rollback()
		resp.Message = "error importing Zat Aktif Produk: " + err.Error()
		resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	// ---- Sheet: Supplier Produk ----
	if err := importSupplierProduk(f, tx, *batchSize, *adminID, &messageDetailBuilder); err != nil {
		_ = tx.Rollback()
		resp.Message = "error importing Supplier Produk: " + err.Error()
		resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	// ---- Sheet: Grup Produk ----
	if err := importGrupProduk(f, tx, *batchSize, *adminID, &messageDetailBuilder); err != nil {
		_ = tx.Rollback()
		resp.Message = "error importing Grup Produk: " + err.Error()
		resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	// ---- Sheet: Izin Produk ----
	if err := importIzinProduk(f, tx, *batchSize, *adminID, &messageDetailBuilder); err != nil {
		_ = tx.Rollback()
		resp.Message = "error importing Izin Produk: " + err.Error()
		resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	// commit
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		resp.Message = "db commit error: " + err.Error()
		resp.MessageDetail = fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds())
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	// optional update activity in separate tx
	if *logID != "" {
		tx2, err := db.Begin()
		if err == nil {
			_ = updateActivity(tx2, *logID, "IMPORT DATA PRODUCT", "product/view_product_list", "{}")
			_ = tx2.Commit()
		}
	}

	messageDetailBuilder.WriteString(fmt.Sprintf("Execution Time : %.4f seconds", time.Since(start).Seconds()))
	resp.Success = true
	resp.Message = "Import Product Success"
	resp.MessageDetail = messageDetailBuilder.String()

	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("Import Product finished. %s\n", resp.MessageDetail)
}

// ---------------- Sheet handlers ----------------

func importDaftarProduk(f *excelize.File, tx *sql.Tx, batchSize int, adminID int, md *strings.Builder) error {
	sheet := "Daftar Produk"
	rows, err := f.Rows(sheet)
	if err != nil {
		// sheet may not exist -> just skip quietly
		fmt.Println("sheet gak ada")
		return fmt.Errorf("sheet '%s' not found: %w", sheet, err)
	}
	defer rows.Close()

	colsList := []string{
		"product_id", "product_name", "product_alias", "product_brand", "product_code",
		"product_status_id", "principal_id", "principal_division_id", "finished_drug_code", "old_code",
		"catalogue_code", "product_code_principal", "classification_id", "product_class", "division_id",
		"packaging", "size", "temperature_requirement", "expired_threshold", "length",
		"length_unit", "width", "width_unit", "height", "height_unit",
		"weight", "weight_unit", "volume", "volume_unit", "biggest_conv",
		"biggest_unit", "smallest_conv", "smallest_unit", "sale_unit", "manufacturer",
		"default_margin_principal", "lock_discount", "lock_sale", "stock_level_product", "form_id",
		"remark", "is_need_expired", "required_serial_number", "product_het", "default_hna",
		"createdAt", "createdBy",
	}

	batchRows := [][]interface{}{}
	succeed := 0
	failed := 0
	currentRow := 0
	rowIndex := 0
	uniqueName := map[string]bool{}
	uniqueCode := map[string]bool{}

	for rows.Next() {
		rowIndex++
		cols, err := rows.Columns()
		if err != nil {
			fmt.Println("col gak ada")
			return err
		}
		// skip header
		if rowIndex < 4 {
			continue
		}
		currentRow = rowIndex

		// ensure length
		for len(cols) < 44 {
			cols = append(cols, "")
		}

		// helper to get normalized cell
		getCol := func(idx int) *string {
			if idx < len(cols) {
				return checkIsTrueEmpty(cols[idx])
			}
			return nil
		}

		// stop if first col empty
		if getCol(0) == nil {
			continue
		}

		productNamePtr := getCol(0)
		if productNamePtr == nil {
			continue
		}
		productName := *productNamePtr

		if productName == "Free Text" {
			continue
		}

		productAlias := ""
		if p := getCol(1); p != nil {
			productAlias = *p
		}
		productBrand := ""
		if p := getCol(2); p != nil {
			productBrand = *p
		}
		productCodeStr := ""
		if p := getCol(3); p != nil {
			productCodeStr = *p
		}
		// duplicate code check (query)
		dupName, err := checkDuplicate(tx, "list_product", "product_code", productCodeStr)
		if err != nil {
			fmt.Println("error duplicate")
			return err
		}
		if (productCodeStr != "" && uniqueName[productCodeStr]) || dupName {
			failed++
			fmt.Println("Duplicate code: ", productCodeStr)
			md.WriteString(fmt.Sprintf(" [%d Duplikat Code]", currentRow))
			continue
		}
		uniqueName[productCodeStr] = true

		// product_id is int from product_code in PHP
		var productID interface{} = nil
		if productCodeStr != "" {
			if n, err := strconv.Atoi(productCodeStr); err == nil {
				productID = n
			} else {
				// if cannot parse, skip (mirror earlier decision)
				md.WriteString(fmt.Sprintf("[%d Invalid product_code -> skip]", currentRow))
				fmt.Println("invalid product code: ", productCodeStr)
				failed++
				continue
			}
		} else {
			productID = nil
		}

		// principal and division via checkImportColumn
		principalID := sql.NullInt64{Valid: false}
		if p := getCol(4); p != nil && *p != "" {
			id, err := checkImportColumn(tx, "principal_name", "list_principal", *p, nil)
			if err != nil {
				fmt.Println("gak ada principal")
				return err
			}
			principalID = id
		}

		principalDivisionID := sql.NullInt64{Valid: false}
		if p := getCol(5); p != nil && *p != "" {
			opts := map[string]string{}
			if principalID.Valid {
				opts["principal_id"] = fmt.Sprintf("%d", principalID.Int64)
			}
			id, err := checkImportColumn(tx, "division_name", "list_principal_division", *p, opts)
			if err != nil {
				fmt.Println("gak ada divisi")
				return err
			}
			principalDivisionID = id
		}

		finishedDrugCode := ""
		if p := getCol(6); p != nil {
			finishedDrugCode = *p
		}
		oldCode := ""
		if p := getCol(7); p != nil {
			oldCode = *p
		}
		catalogueCode := ""
		if p := getCol(8); p != nil {
			catalogueCode = *p
		}
		productCodePrincipal := ""
		if p := getCol(9); p != nil {
			productCodePrincipal = *p
		}

		// classification
		var classificationID sql.NullInt64

		if p := getCol(10); p != nil && *p != "" {
			id, err := checkImportColumn(tx, "classification_name", "list_product_classification", *p, nil)
			if err != nil {
				fmt.Println("gak ada clasification")
				return err
			}
			classificationID = id
		} else {
			id, err := checkImportColumn(tx, "classification_name", "list_product_classification", "SUPLEMEN", nil)
			if err != nil {
				return err
			}
			classificationID = id
		}

		productClass := ""
		if p := getCol(11); p != nil {
			productClass = *p
		}

		division := ""
		if p := getCol(12); p != nil {
			division = *p
		}
		productDivision := 3
		if strings.EqualFold(division, "Pharmacy") {
			productDivision = 1
		} else if strings.EqualFold(division, "Hoslab") {
			productDivision = 2
		}

		packaging := ""
		if p := getCol(13); p != nil {
			packaging = *p
		}
		size := ""
		if p := getCol(14); p != nil {
			size = *p
		}
		temperatureRequirement := ""
		if p := getCol(15); p != nil {
			temperatureRequirement = *p
		}
		expiredThreshold := "0"
		if p := getCol(16); p != nil {
			expiredThreshold = denormalizeNumber(p)
		}
		length := denormalizeNumber(getCol(17))
		lengthUnit := sql.NullInt64{Valid: false}
		if p := getCol(18); p != nil && *p != "" {
			id, err := checkImportColumn(tx, "unit_name", "list_unit", *p, nil)
			if err != nil {
				return err
			}
			lengthUnit = id
		}
		width := denormalizeNumber(getCol(19))
		widthUnit := sql.NullInt64{Valid: false}
		if p := getCol(20); p != nil && *p != "" {
			id, err := checkImportColumn(tx, "unit_name", "list_unit", *p, nil)
			if err != nil {
				fmt.Println("gak ada unit")
				return err
			}
			widthUnit = id
		}
		height := denormalizeNumber(getCol(21))
		heightUnit := sql.NullInt64{Valid: false}
		if p := getCol(22); p != nil && *p != "" {
			id, err := checkImportColumn(tx, "unit_name", "list_unit", *p, nil)
			if err != nil {
				return err
			}
			heightUnit = id
		}
		weight := denormalizeNumber(getCol(23))
		weightUnit := sql.NullInt64{Valid: false}
		if p := getCol(24); p != nil && *p != "" {
			id, err := checkImportColumn(tx, "unit_name", "list_unit", *p, nil)
			if err != nil {
				return err
			}
			weightUnit = id
		}
		volume := denormalizeNumber(getCol(25))
		volumeUnit := sql.NullInt64{Valid: false}
		if p := getCol(26); p != nil && *p != "" {
			id, err := checkImportColumn(tx, "unit_name", "list_unit", *p, nil)
			if err != nil {
				return err
			}
			volumeUnit = id
		}
		biggestConv := denormalizeNumber(getCol(27))
		biggestUnit := ""
		if p := getCol(28); p != nil {
			biggestUnit = *p
		}
		smallestConv := denormalizeNumber(getCol(29))
		smallestUnit := ""
		if p := getCol(30); p != nil {
			smallestUnit = *p
		}
		saleUnit := ""
		if p := getCol(31); p != nil {
			saleUnit = *p
		}
		manufacturer := ""
		if p := getCol(32); p != nil {
			manufacturer = *p
		}
		defaultMarginPrincipal := "0"
		if p := getCol(33); p != nil && *p != "" {
			defaultMarginPrincipal = *p
		}
		lockDiscount := 0
		if p := getCol(34); p != nil && strings.EqualFold(*p, "Ya") {
			lockDiscount = 1
		}
		lockSale := 0
		if p := getCol(35); p != nil && strings.EqualFold(*p, "Ya") {
			lockSale = 1
		}
		stockLevelProduct := "0"
		if p := getCol(36); p != nil && *p != "" {
			stockLevelProduct = *p
		}
		formID := sql.NullInt64{Valid: false}
		if p := getCol(37); p != nil && *p != "" {
			id, err := checkImportColumn(tx, "form_name", "list_product_form", *p, nil)
			if err != nil {
				insertSQL := "INSERT INTO list_product_form (form_name, createdAt, createdBy) VALUES (?, ?, ?)"
				createdAt := time.Now().Format("2006-01-02 15:04:05")
				result, insErr := tx.Exec(insertSQL, *p, createdAt, 1)
				if insErr != nil {
					return fmt.Errorf("gagal insert form baru: %w", insErr)
				}
				newID, _ := result.LastInsertId()
				formID = sql.NullInt64{Int64: newID, Valid: true}
			} else {
				formID = id
			}
		}
		remark := ""
		if p := getCol(38); p != nil {
			remark = *p
		}
		isNeedExpired := 0
		if p := getCol(39); p != nil && strings.EqualFold(*p, "Ya") {
			isNeedExpired = 1
		}
		requiredSerialNumber := 0
		if p := getCol(40); p != nil && strings.EqualFold(*p, "Ya") {
			requiredSerialNumber = 1
		}
		productHet := "0"
		if p := getCol(41); p != nil && *p != "" {
			productHet = *p
		}
		defaultHna := "0"
		if p := getCol(42); p != nil && *p != "" {
			defaultHna = *p
		}
		productStatus := 1
		if p := getCol(43); p != nil && strings.EqualFold(*p, "Aktif") {
			productStatus = 2
		}

		// duplicate code check
		if productCodeStr != "" {
			dupCode, err := checkDuplicate(tx, "list_product", "product_code", productCodeStr)
			if err != nil {
				return err
			}
			if uniqueCode[productCodeStr] || dupCode {
				failed++
				fmt.Println("Duplicate code: ", productCodeStr)
				md.WriteString(fmt.Sprintf("[%d Duplikat Kode]", currentRow))
				continue
			}
			uniqueCode[productCodeStr] = true
		}

		// build value row in proper order
		createdAt := time.Now().Format("2006-01-02 15:04:05")
		rowVals := []interface{}{
			productID, // product_id (can be nil)
			productName,
			productAlias,
			productBrand,
			productCodeStr,
			productStatus,
		}
		// principal_id
		if principalID.Valid {
			rowVals = append(rowVals, principalID.Int64)
		} else {
			rowVals = append(rowVals, nil)
		}
		// principal_division_id
		if principalDivisionID.Valid {
			rowVals = append(rowVals, principalDivisionID.Int64)
		} else {
			rowVals = append(rowVals, nil)
		}
		rowVals = append(rowVals,
			finishedDrugCode,
			oldCode,
			catalogueCode,
			productCodePrincipal,
		)
		// classification_id
		if classificationID.Valid {
			rowVals = append(rowVals, classificationID.Int64)
		} else {
			rowVals = append(rowVals, nil)
		}
		rowVals = append(rowVals,
			productClass,
			productDivision,
			packaging,
			size,
			temperatureRequirement,
			expiredThreshold,
			length,
		)
		// length_unit
		if lengthUnit.Valid {
			rowVals = append(rowVals, lengthUnit.Int64)
		} else {
			rowVals = append(rowVals, nil)
		}
		rowVals = append(rowVals, width)
		if widthUnit.Valid {
			rowVals = append(rowVals, widthUnit.Int64)
		} else {
			rowVals = append(rowVals, nil)
		}
		rowVals = append(rowVals, height)
		if heightUnit.Valid {
			rowVals = append(rowVals, heightUnit.Int64)
		} else {
			rowVals = append(rowVals, nil)
		}
		rowVals = append(rowVals, weight)
		if weightUnit.Valid {
			rowVals = append(rowVals, weightUnit.Int64)
		} else {
			rowVals = append(rowVals, nil)
		}
		rowVals = append(rowVals,
			volume,
		)
		if volumeUnit.Valid {
			rowVals = append(rowVals, volumeUnit.Int64)
		} else {
			rowVals = append(rowVals, nil)
		}
		rowVals = append(rowVals,
			biggestConv,
			biggestUnit,
			smallestConv,
			smallestUnit,
			saleUnit,
			manufacturer,
			defaultMarginPrincipal,
			lockDiscount,
			lockSale,
			stockLevelProduct,
		)
		// form_id
		if formID.Valid {
			rowVals = append(rowVals, formID.Int64)
		} else {
			rowVals = append(rowVals, nil)
		}
		createdAt = time.Now().Format("2006-01-02 15:04:05")
		rowVals = append(rowVals,
			remark,
			isNeedExpired,
			requiredSerialNumber,
			productHet,
			defaultHna,
			createdAt,
			adminID,
		)

		batchRows = append(batchRows, rowVals)
		succeed++

		if len(batchRows) >= batchSize {
			base := "INSERT INTO `list_product`"
			q, args := buildMultiInsert(base, colsList, batchRows)
			if _, err := tx.Exec(q, args...); err != nil {
				return fmt.Errorf("error inserting batch to list_product: %w", err)
			}
			batchRows = [][]interface{}{}
		}
	}

	// flush remaining
	if len(batchRows) > 0 {
		base := "INSERT INTO `list_product`"
		q, args := buildMultiInsert(base, colsList, batchRows)
		if _, err := tx.Exec(q, args...); err != nil {
			return fmt.Errorf("error inserting final batch to list_product: %w", err)
		}
		fmt.Println("Sukses insert product")
	}

	md.WriteString("Import worksheet Daftar Produk berhasil :")
	md.WriteString(fmt.Sprintf("- Total %d baris data berhasil disimpan", succeed))
	md.WriteString(fmt.Sprintf("- Total %d baris duplikat data gagal disimpan", failed))
	return nil
}

func importZatAktifProduk(f *excelize.File, tx *sql.Tx, batchSize int, adminID int, md *strings.Builder) error {
	sheet := "Zat Aktif Produk"
	rows, err := f.Rows(sheet)
	if err != nil {
		return fmt.Errorf("sheet '%s' not found: %w", sheet, err)
	}
	defer rows.Close()

	colsList := []string{"product_id", "substance_id", "createdAt", "createdBy"}
	batchRows := [][]interface{}{}
	succeed := 0
	// currentRow := 0
	rowIndex := 0

	for rows.Next() {
		rowIndex++
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		if rowIndex == 1 {
			continue
		}
		// currentRow = rowIndex

		// ensure enough cols
		for len(cols) < 3 {
			cols = append(cols, "")
		}
		getCol := func(idx int) *string {
			if idx < len(cols) {
				return checkIsTrueEmpty(cols[idx])
			}
			return nil
		}
		if getCol(0) == nil {
			continue
		}
		productCode := ""
		if p := getCol(0); p != nil {
			productCode = *p
		}
		// get product_id by name
		row := tx.QueryRow("SELECT product_id FROM list_product WHERE product_code = ? LIMIT 1", productCode)
		var productID int64
		if err := row.Scan(&productID); err != nil {
			// skip if not found
			fmt.Println("product code tidak ada: ", productCode)
			continue
		}

		substances := ""
		if p := getCol(2); p != nil {
			substances = *p
		}
		if substances == "" {
			continue
		}
		// split by comma
		items := strings.Split(substances, ",")
		for _, it := range items {
			sub := strings.TrimSpace(it)
			if sub == "" {
				continue
			}
			// import substance to list_substance if not exists
			subID, err := checkImportColumn(tx, "substance_name", "list_substance", sub, nil)
			if err != nil {
				createdAt := time.Now().Format("2006-01-02 15:04:05")
				insertSQL := "INSERT INTO list_substance (substance_name, createdAt, createdBy) VALUES (?, ?, ?)"
				result, insErr := tx.Exec(insertSQL, sub, createdAt, 1)
				if insErr != nil {
					return fmt.Errorf("gagal insert substance baru: %w", insErr)
				}
				newID, _ := result.LastInsertId()
				subID = sql.NullInt64{Int64: newID}
			}
			createdAt := time.Now().Format("2006-01-02 15:04:05")
			batchRows = append(batchRows, []interface{}{productID, subID.Int64, createdAt, adminID})
			succeed++
			if len(batchRows) >= batchSize {
				base := "INSERT INTO `rel_product_substance`"
				q, args := buildMultiInsert(base, colsList, batchRows)
				if _, err := tx.Exec(q, args...); err != nil {
					return fmt.Errorf("error inserting batch to rel_product_substance: %w", err)
				}
				batchRows = [][]interface{}{}
			}
		}
	}

	if len(batchRows) > 0 {
		base := "INSERT INTO `rel_product_substance`"
		q, args := buildMultiInsert(base, colsList, batchRows)
		if _, err := tx.Exec(q, args...); err != nil {
			return fmt.Errorf("error inserting final batch to rel_product_substance: %w", err)
		}
		fmt.Println("Sukses insert zat aktif")
	}

	md.WriteString("Import worksheet Zat Aktif Produk berhasil :")
	md.WriteString(fmt.Sprintf("- Total %d baris data berhasil disimpan", succeed))
	return nil
}

func importSupplierProduk(f *excelize.File, tx *sql.Tx, batchSize int, adminID int, md *strings.Builder) error {
	sheet := "Supplier Produk"
	rows, err := f.Rows(sheet)
	if err != nil {
		return fmt.Errorf("sheet '%s' not found: %w", sheet, err)
	}
	defer rows.Close()

	colsList := []string{"product_id", "supplier_id", "flag_id", "createdAt", "createdBy"}
	batchRows := [][]interface{}{}
	succeed := 0
	rowIndex := 0

	for rows.Next() {
		rowIndex++
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		if rowIndex == 1 {
			continue
		}
		// ensure cols
		for len(cols) < 4 {
			cols = append(cols, "")
		}
		getCol := func(idx int) *string {
			if idx < len(cols) {
				return checkIsTrueEmpty(cols[idx])
			}
			return nil
		}
		if getCol(0) == nil {
			continue
		}
		productCode := ""
		if p := getCol(0); p != nil {
			productCode = *p
		}
		// find product by code
		row := tx.QueryRow("SELECT product_id FROM list_product WHERE product_code = ? LIMIT 1", productCode)
		var productID int64
		if err := row.Scan(&productID); err != nil {
			fmt.Println("product code di insert supplier tidak ada: ", productCode)
			continue
		}
		supplierName := ""
		if p := getCol(2); p != nil {
			supplierName = *p
		}
		// get supplier id by name (LIKE)
		rowS := tx.QueryRow("SELECT supplier_id FROM list_supplier WHERE supplier_name LIKE ? LIMIT 1", "%"+supplierName+"%")
		var supplierID int64
		if err := rowS.Scan(&supplierID); err != nil {
			fmt.Println("supplier tidak ditemukan: ", supplierName)
			continue
		}
		flagName := ""
		if p := getCol(3); p != nil {
			flagName = strings.ToLower(*p)
		}
		flagID := 3
		if flagName == "reguler" {
			flagID = 1
		} else if flagName == "konsinyasi" {
			flagID = 2
		}
		createdAt := time.Now().Format("2006-01-02 15:04:05")
		batchRows = append(batchRows, []interface{}{productID, supplierID, flagID, createdAt, adminID})
		succeed++
		if len(batchRows) >= batchSize {
			base := "INSERT INTO `rel_product_supplier`"
			q, args := buildMultiInsert(base, colsList, batchRows)
			if _, err := tx.Exec(q, args...); err != nil {
				return fmt.Errorf("error inserting batch to rel_product_supplier: %w", err)
			}
			batchRows = [][]interface{}{}
		}
	}

	if len(batchRows) > 0 {
		base := "INSERT INTO `rel_product_supplier`"
		q, args := buildMultiInsert(base, colsList, batchRows)
		if _, err := tx.Exec(q, args...); err != nil {
			return fmt.Errorf("error inserting final batch to rel_product_supplier: %w", err)
		}
		fmt.Println("Sukses insert supplier product")
	}

	md.WriteString("Import worksheet Supplier Produk berhasil :")
	md.WriteString(fmt.Sprintf("- Total %d baris data berhasil disimpan", succeed))
	return nil
}

func importGrupProduk(f *excelize.File, tx *sql.Tx, batchSize int, adminID int, md *strings.Builder) error {
	sheet := "Grup Produk"
	rows, err := f.Rows(sheet)
	if err != nil {
		return fmt.Errorf("sheet '%s' not found: %w", sheet, err)
	}
	defer rows.Close()

	colsList := []string{"product_id", "tag_id", "assigned_date", "createdBy"}
	batchRows := [][]interface{}{}
	succeed := 0
	rowIndex := 0

	for rows.Next() {
		rowIndex++
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		if rowIndex == 1 {
			continue
		}
		for len(cols) < 3 {
			cols = append(cols, "")
		}
		getCol := func(idx int) *string {
			if idx < len(cols) {
				return checkIsTrueEmpty(cols[idx])
			}
			return nil
		}
		if getCol(0) == nil {
			continue
		}
		productCode := ""
		if p := getCol(0); p != nil {
			productCode = *p
		}
		row := tx.QueryRow("SELECT product_id FROM list_product WHERE product_code = ? LIMIT 1", productCode)
		var productID int64
		if err := row.Scan(&productID); err != nil {
			fmt.Println("Produc code di group tidak ditemukan: ", productCode)
			continue
		}
		groupProduct := ""
		if p := getCol(2); p != nil {
			groupProduct = *p
		}
		if strings.Contains(groupProduct, ",") {
			// pick last tag segment (mimic PHP splitting)
			parts := strings.Split(groupProduct, ",")
			groupProduct = strings.TrimSpace(parts[len(parts)-1])
		}
		// find tag id
		rowT := tx.QueryRow("SELECT tag_id FROM list_tag WHERE tag_name LIKE ? LIMIT 1", "%"+groupProduct+"%")
		var tagID int64
		if err := rowT.Scan(&tagID); err != nil {
			insertSQL := "INSERT INTO list_tag (tag_name, tag_type_id, createdAt, createdBy) VALUES (?, ?, ?, ?)"
			createdAt := time.Now().Format("2006-01-02 15:04:05")
			result, insErr := tx.Exec(insertSQL, groupProduct, 2, createdAt, 1)
			if insErr != nil {
				return fmt.Errorf("gagal insert tag baru: %w", insErr)
			}
			newID, _ := result.LastInsertId()
			tagID = newID
		}
		assignedDate := time.Now().Format("2006-01-02 15:04:05")
		batchRows = append(batchRows, []interface{}{productID, tagID, assignedDate, adminID})
		succeed++
		if len(batchRows) >= batchSize {
			base := "INSERT INTO `rel_product_tag`"
			q, args := buildMultiInsert(base, colsList, batchRows)
			if _, err := tx.Exec(q, args...); err != nil {
				return fmt.Errorf("error inserting batch to rel_product_tag: %w", err)
			}
			batchRows = [][]interface{}{}
		}
	}

	if len(batchRows) > 0 {
		base := "INSERT INTO `rel_product_tag`"
		q, args := buildMultiInsert(base, colsList, batchRows)
		if _, err := tx.Exec(q, args...); err != nil {
			return fmt.Errorf("error inserting final batch to rel_product_tag: %w", err)
		}
		fmt.Println("Sukses insert grup product")
	}

	md.WriteString("Import worksheet Grup Produk berhasil :")
	md.WriteString(fmt.Sprintf("- Total %d baris data berhasil disimpan", succeed))
	return nil
}

func importIzinProduk(f *excelize.File, tx *sql.Tx, batchSize int, adminID int, md *strings.Builder) error {
	sheet := "Izin Produk"
	rows, err := f.Rows(sheet)
	if err != nil {
		return fmt.Errorf("sheet '%s' not found: %w", sheet, err)
	}
	defer rows.Close()

	colsList := []string{"license_type_id", "license_name", "license_number", "effective_date", "expired_date", "createdAt", "createdBy", "product_id", "license_status_id"}
	batchRows := [][]interface{}{}
	succeed := 0
	rowIndex := 0

	for rows.Next() {
		rowIndex++
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		if rowIndex == 1 {
			continue
		}
		for len(cols) < 6 {
			cols = append(cols, "")
		}
		getCol := func(idx int) *string {
			if idx < len(cols) {
				return checkIsTrueEmpty(cols[idx])
			}
			return nil
		}
		if getCol(0) == nil {
			continue
		}
		productName := ""
		if p := getCol(0); p != nil {
			productName = *p
		}
		// get product id
		row := tx.QueryRow("SELECT product_id FROM list_product WHERE product_name = ? LIMIT 1", productName)
		var productID int64
		if err := row.Scan(&productID); err != nil {
			fmt.Println("code di product gak ada", productName)
			continue
		}
		licenseType := 3
		licenseName := ""
		if p := getCol(2); p != nil {
			licenseName = *p
		}
		licenseNumber := ""
		if p := getCol(3); p != nil {
			licenseNumber = *p
		}
		var effectiveDate interface{} = nil
		if p := getCol(4); p != nil {
			effectiveDate = parseDateForSQL(p)
		}
		var expiredDate interface{} = nil
		if p := getCol(5); p != nil {
			expiredDate = parseDateForSQL(p)
		}

		createdAt := time.Now().Format("2006-01-02 15:04:05")
		licenseStatus := 1
		batchRows = append(batchRows, []interface{}{licenseType, licenseName, licenseNumber, effectiveDate, expiredDate, createdAt, adminID, productID, licenseStatus})
		succeed++
		if len(batchRows) >= batchSize {
			base := "INSERT INTO `list_license`"
			q, args := buildMultiInsert(base, colsList, batchRows)
			if _, err := tx.Exec(q, args...); err != nil {
				return fmt.Errorf("error inserting batch to list_license: %w", err)
			}
			batchRows = [][]interface{}{}
		}

	}

	if len(batchRows) > 0 {
		base := "INSERT INTO `list_license`"
		q, args := buildMultiInsert(base, colsList, batchRows)
		if _, err := tx.Exec(q, args...); err != nil {
			return fmt.Errorf("error inserting final batch to list_license: %w", err)
		}
		fmt.Println("Sukses insert izin product")
	}

	md.WriteString("Import worksheet Izin Produk berhasil :")
	md.WriteString(fmt.Sprintf("- Total %d baris data berhasil disimpan", succeed))
	return nil
}
