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

func RunImportTransferOutstandingCmd(args []string) {
	fs := flag.NewFlagSet("transfer", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/transfer.xlsx", "path to xlsx file")
	dsn := fs.String("dsn", "", "mysql DSN, e.g. user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true")
	adminID := fs.Int("admin-id", 1, "requestedBy admin id")
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
	invoiceCache := make(map[string]*InvoiceTransferData)
	branchCache := make(map[string]int64)
	depositCache := make(map[string]int64)

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
		// indices: 0 transfer_number, 1 invoice_number, 2 branch_origin, 3 branch_destination,
		//          4 transfer_note, 5 transfer_date, 7 transfer_type, 8 snapshot_amount, 9 snapshot_settlement
		if len(rowData) < 8 {
			fmt.Println("column lebih kecil dari 8")
			continue
		}

		transferNumberPtr := getCol(0)
		if transferNumberPtr == nil {
			break // stop if no transfer number
		}

		invoiceNumberPtr := getCol(1)
		branchOriginPtr := getCol(2)
		branchDestinationPtr := getCol(3)
		transferNotePtr := getCol(4)
		transferDatePtr := getCol(5)
		transferTypePtr := getCol(7)
		snapshotAmountPtr := getCol(8)
		snapshotSettlementPtr := getCol(9)

		if invoiceNumberPtr == nil || branchOriginPtr == nil || branchDestinationPtr == nil || transferTypePtr == nil {
			fmt.Println("invoice number kosong")
			continue
		}

		transferNumber := strings.TrimSpace(*transferNumberPtr)
		invoiceNumber := strings.TrimSpace(*invoiceNumberPtr)
		branchOriginName := strings.TrimSpace(*branchOriginPtr)
		branchDestinationName := strings.TrimSpace(*branchDestinationPtr)
		transferType := strings.ToLower(strings.TrimSpace(*transferTypePtr))

		transferNote := ""
		if transferNotePtr != nil {
			transferNote = strings.TrimSpace(*transferNotePtr)
		}

		// Parse transfer date
		transferDate := parseDateForSQL(transferDatePtr)
		if transferDate == "" {
			transferDate = time.Now().Format("2006-01-02")
		}

		// Get or cache branch origin
		var branchOriginID int64
		if cached, ok := branchCache[branchOriginName]; ok {
			branchOriginID = cached
		} else {
			err = tx.QueryRow(`
				SELECT branch_id 
				FROM list_branch 
				WHERE branch_name = ? 
				LIMIT 1
			`, branchOriginName).Scan(&branchOriginID)

			if err == sql.ErrNoRows {
				fmt.Printf("Branch origin not found: %s\n", branchOriginName)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying branch origin: " + err.Error()
				goto FINISH
			}
			branchCache[branchOriginName] = branchOriginID
		}

		// Get or cache branch destination
		var branchDestinationID int64
		if cached, ok := branchCache[branchDestinationName]; ok {
			branchDestinationID = cached
		} else {
			err = tx.QueryRow(`
				SELECT branch_id 
				FROM list_branch 
				WHERE branch_name = ? 
				LIMIT 1
			`, branchDestinationName).Scan(&branchDestinationID)

			if err == sql.ErrNoRows {
				fmt.Printf("Branch destination not found: %s\n", branchDestinationName)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying branch destination: " + err.Error()
				goto FINISH
			}
			branchCache[branchDestinationName] = branchDestinationID
		}

		// Process based on transfer type
		if strings.Contains(transferType, "deposit") {
			// Get or cache deposit
			var depositID int64
			if cached, ok := depositCache[invoiceNumber]; ok {
				depositID = cached
			} else {
				err = tx.QueryRow(`
					SELECT deposit_id 
					FROM list_outlet_deposit 
					WHERE deposit_number = ? 
					LIMIT 1
				`, invoiceNumber).Scan(&depositID)

				if err == sql.ErrNoRows {
					log.Printf("Missing Deposit: %s\n", invoiceNumber)
					continue
				} else if err != nil {
					_ = tx.Rollback()
					resp.Message = "error querying deposit: " + err.Error()
					goto FINISH
				}
				depositCache[invoiceNumber] = depositID
			}

			// INSERT list_deposit_transfer
			res, err := tx.Exec(`
				INSERT INTO list_deposit_transfer (
					branch_source_id, branch_destination_id, request_number,
					transfer_note, requestedAt, requestedBy
				) VALUES (?, ?, ?, ?, ?, ?)
			`, branchOriginID, branchDestinationID, transferNumber, transferNote, transferDate, *adminID)

			if err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting deposit transfer: " + err.Error()
				goto FINISH
			}
			transferID, _ := res.LastInsertId()

			// INSERT rel_deposit_transfer_transaction
			_, err = tx.Exec(`
				INSERT INTO rel_deposit_transfer_transaction (deposit_transfer_id, deposit_id)
				VALUES (?, ?)
			`, transferID, depositID)

			if err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting deposit transfer transaction: " + err.Error()
				goto FINISH
			}

			insertedCount++

		} else if strings.Contains(transferType, "outstanding") {
			// Get or cache invoice
			var invoice *InvoiceTransferData
			if cached, ok := invoiceCache[invoiceNumber]; ok {
				invoice = cached
			} else {
				var invData InvoiceTransferData
				err = tx.QueryRow(`
					SELECT sales_invoice_id, outlet_id
					FROM list_sales_invoice
					WHERE sales_invoice_number = ?
					LIMIT 1
				`, invoiceNumber).Scan(&invData.SalesInvoiceID, &invData.OutletID)

				if err == sql.ErrNoRows {
					log.Printf("Missing Invoice: %s\n", invoiceNumber)
					continue
				} else if err != nil {
					_ = tx.Rollback()
					resp.Message = "error querying invoice: " + err.Error()
					goto FINISH
				}
				invoice = &invData
				invoiceCache[invoiceNumber] = invoice
			}

			snapshotAmount := denormFloat(snapshotAmountPtr)
			snapshotSettlement := denormFloat(snapshotSettlementPtr)

			// INSERT list_outstanding_transfer
			res, err := tx.Exec(`
				INSERT INTO list_outstanding_transfer (
					branch_source_id, branch_destination_id, request_number,
					transfer_note, requestedAt, requestedBy
				) VALUES (?, ?, ?, ?, ?, ?)
			`, branchOriginID, branchDestinationID, transferNumber, transferNote, transferDate, *adminID)

			if err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting outstanding transfer: " + err.Error()
				goto FINISH
			}
			transferID, _ := res.LastInsertId()

			// INSERT rel_outstanding_transfer_transaction
			_, err = tx.Exec(`
				INSERT INTO rel_outstanding_transfer_transaction (
					outstanding_transfer_id, sales_invoice_id, outlet_id,
					snapshot_amount, snapshot_settlement
				) VALUES (?, ?, ?, ?, ?)
			`, transferID, invoice.SalesInvoiceID, invoice.OutletID, snapshotAmount, snapshotSettlement)

			if err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting outstanding transfer transaction: " + err.Error()
				goto FINISH
			}

			insertedCount++
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		resp.Message = "db commit error: " + err.Error()
		goto FINISH
	}

	resp.Success = true
	resp.Message = "Import Transfer Outstanding Success"
	resp.MessageDetail = fmt.Sprintf("Total %d transfers inserted. Execution Time: %.4fs", insertedCount, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import transfer outstanding complete: %d transfers, time=%.4fs\n", insertedCount, time.Since(start).Seconds())
}

// Helper struct
type InvoiceTransferData struct {
	SalesInvoiceID int64
	OutletID       int64
}
