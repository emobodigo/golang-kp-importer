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

// SKB Type constants
const (
	SKBTypeMutasiPusatCabang         = 3
	SKBTypeReturBarangRegulerKePusat = 10
	SKBTypeReturBarangRusakKePusat   = 2
)

func RunImportSKBCentralIntransitCmd(args []string) {
	fs := flag.NewFlagSet("intransit", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/intransit.xlsx", "path to xlsx file")
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
	branchIssuerCache := make(map[string]*BranchSKBData)
	branchDestinationCache := make(map[string]*BranchSKBData)
	skbExistsCache := make(map[string]bool)

	// Batch containers
	cols := []string{
		"skb_number", "skb_date", "skb_status_id", "skb_type_id",
		"issuer_warehouse_id", "issuer_type_id", "issuer_id", "issuer",
		"destination_type_id", "destination_id", "destination", "skb_note",
		"is_complete", "createdAt", "createdBy", "division_id",
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
		// indices: 0 skb_number, 1 skb_date, 2 skb_type, 3 issuer_warehouse, 4 issuer_name, 5 destination_name, 6 note
		if len(rowData) < 6 {
			fmt.Println("column kurang dari 6")
			continue
		}

		skbNumberPtr := getCol(0)
		if skbNumberPtr == nil {
			fmt.Println("skb number")
			continue
		}

		skbNumber := strings.TrimSpace(*skbNumberPtr)

		// Check if SKB already exists (cache)
		if _, exists := skbExistsCache[skbNumber]; !exists {
			var existingID int64
			err = tx.QueryRow(`
				SELECT skb_id 
				FROM list_skb 
				WHERE skb_number = ? 
				LIMIT 1
			`, skbNumber).Scan(&existingID)

			if err == nil {
				skbExistsCache[skbNumber] = true
			} else if err == sql.ErrNoRows {
				skbExistsCache[skbNumber] = false
			} else {
				_ = tx.Rollback()
				resp.Message = "error checking skb existence: " + err.Error()
				goto FINISH
			}
		}

		if skbExistsCache[skbNumber] {
			fmt.Printf("SKB already exists, skipping: %s\n", skbNumber)
			continue
		}

		skbDatePtr := getCol(1)
		skbTypePtr := getCol(2)
		issuerWarehousePtr := getCol(3)
		issuerNamePtr := getCol(4)
		destinationNamePtr := getCol(5)
		notePtr := getCol(6)
		divisionPtr := getCol(7)

		// Parse SKB date
		skbDate := parseDateForSQL(skbDatePtr)
		if skbDate == "" {
			skbDate = time.Now().Format("2006-01-02")
		}

		// Parse SKB type
		skbTypeID := SKBTypeMutasiPusatCabang // default
		if skbTypePtr != nil {
			typeStr := strings.ToLower(strings.TrimSpace(*skbTypePtr))
			if strings.Contains(typeStr, "mutasi pusat cabang") {
				skbTypeID = SKBTypeMutasiPusatCabang
			} else if strings.Contains(typeStr, "retur barang reguler") {
				skbTypeID = SKBTypeReturBarangRegulerKePusat
			} else if strings.Contains(typeStr, "retur barang rusak") {
				skbTypeID = SKBTypeReturBarangRusakKePusat
			}
		}

		// Get or cache issuer branch
		if issuerNamePtr == nil {
			fmt.Println("issuer name is nil")
			continue
		}
		issuerName := strings.TrimSpace(*issuerNamePtr)

		var issuerBranch *BranchSKBData
		if cached, ok := branchIssuerCache[issuerName]; ok {
			issuerBranch = cached
		} else {
			var branchData BranchSKBData
			err = tx.QueryRow(`
				SELECT branch_id, branch_name
				FROM list_branch
				WHERE branch_name = ?
				LIMIT 1
			`, issuerName).Scan(&branchData.BranchID, &branchData.BranchName)

			if err == sql.ErrNoRows {
				fmt.Printf("Issuer branch not found: %s\n", issuerName)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying issuer branch: " + err.Error()
				goto FINISH
			}
			issuerBranch = &branchData
			branchIssuerCache[issuerName] = issuerBranch
		}

		// Get or cache destination branch
		if destinationNamePtr == nil {
			fmt.Println("destination name is nil")
			continue
		}
		destinationName := strings.TrimSpace(*destinationNamePtr)

		var destinationBranch *BranchSKBData
		if cached, ok := branchDestinationCache[destinationName]; ok {
			destinationBranch = cached
		} else {
			var branchData BranchSKBData
			err = tx.QueryRow(`
				SELECT branch_id, branch_name
				FROM list_branch
				WHERE branch_name = ?
				LIMIT 1
			`, destinationName).Scan(&branchData.BranchID, &branchData.BranchName)

			if err == sql.ErrNoRows {
				fmt.Printf("Destination branch not found: %s\n", destinationName)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying destination branch: " + err.Error()
				goto FINISH
			}
			destinationBranch = &branchData
			branchDestinationCache[destinationName] = destinationBranch
		}

		// Parse issuer warehouse type
		issuerWarehouseTypeID := 1 // default gudang aktif/reguler
		if issuerWarehousePtr != nil {
			warehouseStr := strings.ToLower(strings.TrimSpace(*issuerWarehousePtr))
			if strings.Contains(warehouseStr, "gudang aktif") || strings.Contains(warehouseStr, "reguler") {
				issuerWarehouseTypeID = 1
			} else if strings.Contains(warehouseStr, "gudang barang rusak") {
				issuerWarehouseTypeID = 2
			}
		}

		// Get warehouse ID
		var issuerWarehouseID int64
		err = tx.QueryRow(`
			SELECT warehouse_id
			FROM list_warehouse
			WHERE branch_id = ? AND warehouse_type_id = ?
			LIMIT 1
		`, issuerBranch.BranchID, issuerWarehouseTypeID).Scan(&issuerWarehouseID)

		if err == sql.ErrNoRows {
			fmt.Printf("Warehouse not found for branch %s with type %d\n", issuerName, issuerWarehouseTypeID)
			continue
		} else if err != nil {
			_ = tx.Rollback()
			resp.Message = "error querying warehouse: " + err.Error()
			goto FINISH
		}

		// Parse note
		note := ""
		if notePtr != nil {
			note = strings.TrimSpace(*notePtr)
		}

		divisionId := 1
		if *divisionPtr == "Hoslab" {
			divisionId = 2
		}

		createdAt := time.Now().Format("2006-01-02 15:04:05")

		// Prepare row values in same order as cols
		rowVals := []interface{}{
			skbNumber,                    // skb_number
			skbDate,                      // skb_date
			3,                            // skb_status_id (3 = intransit)
			skbTypeID,                    // skb_type_id
			issuerWarehouseID,            // issuer_warehouse_id
			1,                            // issuer_type_id (1 = branch)
			issuerBranch.BranchID,        // issuer_id
			issuerBranch.BranchName,      // issuer
			1,                            // destination_type_id (1 = branch)
			destinationBranch.BranchID,   // destination_id
			destinationBranch.BranchName, // destination
			note,                         // skb_note
			1,                            // is_complete
			createdAt,                    // createdAt
			*adminID,                     // createdBy
			divisionId,
		}
		batchRows = append(batchRows, rowVals)

		// Flush batch when size reached
		if len(batchRows) >= *batchSize {
			base := "INSERT INTO `list_skb`"
			q, sqlArgs := buildMultiInsert(base, cols, batchRows)
			if _, err := tx.Exec(q, sqlArgs...); err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting batch to list_skb: " + err.Error()
				goto FINISH
			}
			insertedCount += len(batchRows)
			batchRows = [][]interface{}{}
		}
	}

	// Flush remaining
	if len(batchRows) > 0 {
		base := "INSERT INTO `list_skb`"
		q, sqlArgs := buildMultiInsert(base, cols, batchRows)
		if _, err := tx.Exec(q, sqlArgs...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final batch to list_skb: " + err.Error()
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
	resp.Message = "Import SKB Central Intransit Success"
	resp.MessageDetail = fmt.Sprintf("Total %d rows inserted. Execution Time: %.4fs", insertedCount, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import skb central intransit complete: %d rows, time=%.4fs\n", insertedCount, time.Since(start).Seconds())
}

// Helper struct
type BranchSKBData struct {
	BranchID   int64
	BranchName string
}
