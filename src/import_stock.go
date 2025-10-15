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

func RunImportInitialStockCmd(args []string) {
	fs := flag.NewFlagSet("stock", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/stock.xlsx", "path to xlsx file")
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

	// batch containers
	txCols := []string{"tx_date", "tx_type_id", "product_id", "warehouse_id", "is_consignment", "unit", "debit", "credit"}
	batchTxRows := [][]interface{}{}
	// rel pending aligns with batchTxRows order; we store batch_id & qty for each pending tx row
	type relPending struct {
		batchID int64
		qty     int64
	}
	batchRelPending := []relPending{}

	insertedCount := 0
	rowIndex := 0

	// cache for existing/inserted product batch (key -> batch_id)
	batchCache := map[string]int64{}

	for r := 1; r < len(rows); r++ { // skip header row (index 0)
		rowIndex++
		cols := rows[r]
		// helper similar to other code
		getCol := func(idx int) *string {
			if idx < len(cols) {
				return checkIsTrueEmpty(cols[idx]) // existing helper in project
			}
			return nil
		}

		// ensure we have at least the expected columns (safe-guard)
		// indices used: 0 branch_code, 2 product_code, 4 batch_number, 5 date, 6 warehouse_name, 7 stock_sale, 10 is_consignment
		if len(cols) < 8 {
			// skip short rows
			continue
		}

		branchCodePtr := getCol(0)
		productCodePtr := getCol(2)
		batchNumberPtr := getCol(4)
		rawDatePtr := getCol(5)
		warehouseNamePtr := getCol(6)
		stockSalePtr := getCol(7)
		// stockUnitPtr := getCol(8) // not used based on original
		// stockSmallPtr := getCol(9) // optional
		isConsignmentPtr := getCol(10)

		if branchCodePtr == nil || productCodePtr == nil {

			continue
		}
		branchCode := *branchCodePtr
		productCode := *productCodePtr
		batchNumber := ""
		if batchNumberPtr != nil {
			batchNumber = *batchNumberPtr
		}

		warehouseName := ""
		if warehouseNamePtr != nil {
			warehouseName = *warehouseNamePtr
		}
		stockSale := denormInt(stockSalePtr) // helper below -> returns 0 if empty/invalid
		if stockSale == 0 {
			// if no qty, skip
			fmt.Println("Stock sale 0: ", productCode)
			continue
		}
		isConsignment := 0
		if isConsignmentPtr != nil && strings.EqualFold(*isConsignmentPtr, "Ya") {
			isConsignment = 1
		}

		// parse expired date to YYYY-MM-DD ("" allowed)
		expiredDate := parseDateForSQL(rawDatePtr)

		// --- lookup branch ---
		var branchID int64
		err = tx.QueryRow("SELECT branch_id FROM list_branch WHERE branch_code = ? LIMIT 1", branchCode).Scan(&branchID)
		if err == sql.ErrNoRows {
			// branch not found -> skip
			fmt.Println("Branch code tidak ditemukan: ", productCode)
			continue
		} else if err != nil {
			_ = tx.Rollback()
			resp.Message = "error querying branch: " + err.Error()
			goto FINISH
		}

		// --- lookup product ---
		var productID int64
		err = tx.QueryRow("SELECT product_id FROM list_product WHERE product_code = ? LIMIT 1", productCode).Scan(&productID)
		if err == sql.ErrNoRows {
			fmt.Println("Product code tidak ditemukan: ", productCode)
			continue
		} else if err != nil {
			_ = tx.Rollback()
			resp.Message = "error querying product: " + err.Error()
			goto FINISH
		}

		// --- get or insert product batch (cache) ---
		batchKey := fmt.Sprintf("%d|%s|%s", productID, batchNumber, expiredDate)
		var batchID int64
		if id, ok := batchCache[batchKey]; ok {
			batchID = id
		} else {
			// try select
			err = tx.QueryRow("SELECT batch_id FROM list_product_batch WHERE product_id = ? AND batch_number = ? AND expired_date = ? LIMIT 1",
				productID, batchNumber, expiredDate).Scan(&batchID)
			if err == sql.ErrNoRows {
				// insert single batch (we need id immediately)
				res, errIns := tx.Exec("INSERT INTO list_product_batch (product_id, batch_number, expired_date, createdAt, createdBy) VALUES (?, ?, ?, NOW(), ?)",
					productID, batchNumber, expiredDate, *adminID)
				if errIns != nil {
					_ = tx.Rollback()
					resp.Message = "error inserting product batch: " + errIns.Error()
					goto FINISH
				}
				li, _ := res.LastInsertId()
				batchID = li
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying product batch: " + err.Error()
				goto FINISH
			}
			batchCache[batchKey] = batchID
		}

		// --- lookup warehouse ---
		var warehouseID int64
		err = tx.QueryRow("SELECT warehouse_id FROM list_warehouse WHERE warehouse_name LIKE ? AND branch_id = ? LIMIT 1", "%"+warehouseName+"%", branchID).Scan(&warehouseID)
		if err == sql.ErrNoRows {
			res, errIns := tx.Exec(`
        		INSERT INTO list_warehouse 
        		(warehouse_name, warehouse_type_id, warehouse_status_id, branch_id, createdAt, createdBy) 
       			 VALUES (?, 1, 2, ?, NOW(), ?)`, warehouseName, branchID, *adminID)
			if errIns != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting new warehouse: " + errIns.Error()
				goto FINISH
			}

			warehouseID, err = res.LastInsertId()
			if err != nil {
				_ = tx.Rollback()
				resp.Message = "error getting inserted warehouse id: " + err.Error()
				goto FINISH
			}

			fmt.Printf("Warehouse baru dibuat: %s (Cabang: %s) dengan ID %d\n", warehouseName, branchCode, warehouseID)
		} else if err != nil {
			_ = tx.Rollback()
			resp.Message = "error querying warehouse: " + err.Error()
			goto FINISH
		}

		// createdAt := time.Now().Format("2006-01-02 15:04:05")
		txDate := "2025-09-29 00:00:00"

		// Prepare tx row values in same order as txCols
		rowVals := []interface{}{
			txDate,        // tx_date
			1,             // tx_type_id (1)
			productID,     // product_id
			warehouseID,   // warehouse_id
			isConsignment, // is_consignment
			1,             // unit (literal 1 as in PHP)
			stockSale,     // debit
			0,             // credit
		}
		batchTxRows = append(batchTxRows, rowVals)
		batchRelPending = append(batchRelPending, relPending{batchID: batchID, qty: stockSale})

		// flush when reached batch size
		if len(batchTxRows) >= *batchSize {
			// insert list_tx batch
			baseTx := "INSERT INTO `list_tx`"
			qTx, argsTx := buildMultiInsert(baseTx, txCols, batchTxRows)
			res, err := tx.Exec(qTx, argsTx...)
			if err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting batch to list_tx: " + err.Error()
				goto FINISH
			}
			firstID, errF := res.LastInsertId()
			if errF != nil {
				_ = tx.Rollback()
				resp.Message = "error getting last insert id for list_tx: " + errF.Error()
				goto FINISH
			}

			// build rel_tx_batch rows using computed tx ids
			relCols := []string{"tx_id", "batch_id", "qty"}
			relRows := [][]interface{}{}
			for idx, pend := range batchRelPending {
				txID := firstID + int64(idx)
				relRows = append(relRows, []interface{}{txID, pend.batchID, pend.qty})
			}
			// insert rel_tx_batch in batch
			if len(relRows) > 0 {
				baseRel := "INSERT INTO `rel_tx_batch`"
				qRel, argsRel := buildMultiInsert(baseRel, relCols, relRows)
				if _, err := tx.Exec(qRel, argsRel...); err != nil {
					_ = tx.Rollback()
					resp.Message = "error inserting batch to rel_tx_batch: " + err.Error()
					goto FINISH
				}
			}

			insertedCount += len(batchTxRows)
			// clear batch
			batchTxRows = [][]interface{}{}
			batchRelPending = []relPending{}
		}
	} // end rows loop

	// flush remaining if any
	if len(batchTxRows) > 0 {
		baseTx := "INSERT INTO `list_tx`"
		qTx, argsTx := buildMultiInsert(baseTx, txCols, batchTxRows)
		res, err := tx.Exec(qTx, argsTx...)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final batch to list_tx: " + err.Error()
			goto FINISH
		}
		firstID, errF := res.LastInsertId()
		if errF != nil {
			_ = tx.Rollback()
			resp.Message = "error getting last insert id for final list_tx: " + errF.Error()
			goto FINISH
		}
		// build rel rows
		relCols := []string{"tx_id", "batch_id", "qty"}
		relRows := [][]interface{}{}
		for idx, pend := range batchRelPending {
			txID := firstID + int64(idx)
			relRows = append(relRows, []interface{}{txID, pend.batchID, pend.qty})
		}
		if len(relRows) > 0 {
			baseRel := "INSERT INTO `rel_tx_batch`"
			qRel, argsRel := buildMultiInsert(baseRel, relCols, relRows)
			if _, err := tx.Exec(qRel, argsRel...); err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting final batch to rel_tx_batch: " + err.Error()
				goto FINISH
			}
		}
		insertedCount += len(batchTxRows)
	}

	// commit
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		resp.Message = "db commit error: " + err.Error()
		goto FINISH
	}

	// update batch_number sync
	if _, err := db.Exec(`
		UPDATE list_tx lt
		LEFT JOIN rel_tx_batch rtb ON rtb.tx_id = lt.tx_id
		LEFT JOIN list_product_batch lpb ON lpb.batch_id = rtb.batch_id
		SET lt.batch_number = lpb.batch_number
		WHERE 1;
	`); err != nil {
		resp.Message = "update batch_number failed: " + err.Error()
		goto FINISH
	}

	resp.Success = true
	resp.Message = "Import Initial Stock Success"
	resp.MessageDetail = fmt.Sprintf("Total %d rows inserted. Execution Time: %.4fs", insertedCount, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import initial stock complete: %d tx rows, time=%.4fs\n", insertedCount, time.Since(start).Seconds())
}
