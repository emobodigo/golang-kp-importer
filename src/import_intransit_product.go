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

// Reference Type constants

func RunImportSKBCentralIntransitProductCmd(args []string) {
	fs := flag.NewFlagSet("intransit-product", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/intransit_product.xlsx", "path to xlsx file")
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

	// Get missing SKB item list (SKBs without items)
	missingSKBItems := make(map[int64]bool)
	queryMissing := `
		SELECT li.skb_id 
		FROM list_skb li 
		LEFT JOIN rel_skb_item lpi ON li.skb_id = lpi.skb_id 
		WHERE lpi.skb_id IS NULL
	`
	rowsMissing, err := db.Query(queryMissing)
	if err != nil {
		resp.Message = "error querying missing skb items: " + err.Error()
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}
	for rowsMissing.Next() {
		var skbID int64
		if err := rowsMissing.Scan(&skbID); err == nil {
			missingSKBItems[skbID] = true
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
	skbCache := make(map[string]*SKBProductData)
	productCache := make(map[string]int64)

	// Batch containers
	cols := []string{
		"skb_id", "product_id", "unit", "qty", "quoted_price",
		"batch_number", "expired_date", "reference_type_id", "reference_id", "is_extra",
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

		// Minimum columns check
		// indices: 0 skb_number, 1 product_code, 3 qty, 4 qty_extra, 5 price, 6 batch_number, 9 expired_date
		if len(rowData) < 7 {
			fmt.Println("column kurang dari 7")
			continue
		}

		skbNumberPtr := getCol(0)
		productCodePtr := getCol(1)
		qtyPtr := getCol(3)
		qtyExtraPtr := getCol(4)
		pricePtr := getCol(5)
		batchNumberPtr := getCol(6)
		expiredDatePtr := getCol(9)

		if skbNumberPtr == nil || productCodePtr == nil {
			fmt.Println("skb number nil")
			continue
		}

		skbNumber := strings.TrimSpace(*skbNumberPtr)
		productCode := strings.TrimSpace(*productCodePtr)

		// Get or cache SKB
		var skb *SKBProductData
		if cached, ok := skbCache[skbNumber]; ok {
			skb = cached
		} else {
			var skbID int64
			var typeId int64
			err = tx.QueryRow(`
				SELECT skb_id, skb_type_id 
				FROM list_skb 
				WHERE skb_number = ? 
				LIMIT 1
			`, skbNumber).Scan(&skbID, &typeId)

			if err == sql.ErrNoRows {
				fmt.Printf("SKB not found: %s\n", skbNumber)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying skb: " + err.Error()
				goto FINISH
			}
			skb = &SKBProductData{SKBID: skbID, TypeId: typeId}
			skbCache[skbNumber] = skb
		}

		// Check if SKB is in missing list
		if !missingSKBItems[skb.SKBID] {
			fmt.Println("skb in missing list")
			continue
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
		price := denormFloat(pricePtr)

		batchNumber := ""
		if batchNumberPtr != nil {
			batchNumber = strings.TrimSpace(*batchNumberPtr)
		}

		expiredDate := parseDateForSQL(expiredDatePtr)

		var referenceTypeId sql.NullInt64

		if skb.TypeId == 3 {
			referenceTypeId = sql.NullInt64{Int64: 2}
		}

		// Main product entry
		if qty > 0 {
			rowVals := []interface{}{
				skb.SKBID,       // skb_id
				productID,       // product_id
				1,               // unit (always 1)
				qty,             // qty
				price,           // quoted_price
				batchNumber,     // batch_number
				expiredDate,     // expired_date
				referenceTypeId, // reference_type_id
				nil,             // reference_id
				0,               // is_extra (0 for main)
			}
			batchRows = append(batchRows, rowVals)
		}

		// Extra product entry
		if qtyExtra > 0 {
			rowVals := []interface{}{
				skb.SKBID,       // skb_id
				productID,       // product_id
				1,               // unit
				qtyExtra,        // qty
				price,           // quoted_price
				batchNumber,     // batch_number
				expiredDate,     // expired_date
				referenceTypeId, // reference_type_id
				nil,             // reference_id
				1,               // is_extra (1 for extra)
			}
			batchRows = append(batchRows, rowVals)
		}

		// Flush batch when size reached
		if len(batchRows) >= *batchSize {
			base := "INSERT INTO `rel_skb_item`"
			q, sqlArgs := buildMultiInsert(base, cols, batchRows)
			if _, err := tx.Exec(q, sqlArgs...); err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting batch to rel_skb_item: " + err.Error()
				goto FINISH
			}
			insertedCount += len(batchRows)
			batchRows = [][]interface{}{}
		}
	}

	// Flush remaining
	if len(batchRows) > 0 {
		base := "INSERT INTO `rel_skb_item`"
		q, sqlArgs := buildMultiInsert(base, cols, batchRows)
		if _, err := tx.Exec(q, sqlArgs...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final batch to rel_skb_item: " + err.Error()
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
	resp.Message = "Import SKB Central Intransit Product Success"
	resp.MessageDetail = fmt.Sprintf("Total %d items inserted. Execution Time: %.4fs", insertedCount, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import skb central intransit product complete: %d items, time=%.4fs\n", insertedCount, time.Since(start).Seconds())
}

// Helper struct
type SKBProductData struct {
	SKBID  int64
	TypeId int64
}
