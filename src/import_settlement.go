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

func RunImportSettlementCmd(args []string) {
	fs := flag.NewFlagSet("settlement", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/settlement.xlsx", "path to xlsx file")
	dsn := fs.String("dsn", "", "mysql DSN, e.g. user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true")
	adminID := fs.Int("admin-id", 1, "createdBy admin id")
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
	invoiceCache := make(map[string]*InvoiceSettlementData)
	regionCache := make(map[int64]*RegionData)
	giroCache := make(map[string]*GiroData)

	// Settlement giro aggregation
	type GiroInvoiceItem struct {
		SalesInvoiceID   int64
		SettlementAmount float64
		GiroAmount       float64
	}

	type SettlementGiroGroup struct {
		GiroID                int64
		GiroNumber            string
		GiroDueDate           string
		PaymentMethod         int
		DTHDate               string
		Collector             int64
		BranchID              int64
		RegionID              int64
		OutletID              int64
		TotalSettlementAmount float64
		TotalGiroAmount       float64
		InvoiceList           []GiroInvoiceItem
	}

	settlementGiroList := make(map[int64]*SettlementGiroGroup)
	rowIndex := 0
	insertedCount := 0

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
		if len(rowData) < 12 {
			fmt.Println("column less than 12")
			continue
		}

		dthNumberPtr := getCol(0)
		if *dthNumberPtr == "Freetext" {
			continue
		}
		dthTypePtr := getCol(1)
		dthDatePtr := getCol(2)
		settlementAmountPtr := getCol(9)
		paymentMethodPtr := getCol(10)
		invoiceNumberPtr := getCol(11)
		cashAmountPtr := getCol(12)
		transferAmountPtr := getCol(13)
		giroAmountPtr := getCol(14)
		giroNumberPtr := getCol(15)

		if invoiceNumberPtr == nil {
			fmt.Println("invoice number nil")
			continue
		}

		invoiceNumber := strings.TrimSpace(*invoiceNumberPtr)

		// Parse DTH type
		dthTypeID := 2
		if dthTypePtr != nil {
			dthTypeStr := strings.ToLower(strings.TrimSpace(*dthTypePtr))
			if strings.Contains(dthTypeStr, "dalam kota") {
				dthTypeID = 1
			}
		}
		_ = dthTypeID // not used in inserts but parsed as in PHP

		// Parse DTH date
		dthDate := parseDateForSQL(dthDatePtr)
		if dthDate == "" {
			dthDate = time.Now().Format("2006-01-02")
		}

		settlementAmount := denormFloat(settlementAmountPtr)
		cashAmount := denormFloat(cashAmountPtr)
		transferAmount := denormFloat(transferAmountPtr)
		giroAmount := denormFloat(giroAmountPtr)

		// Get or cache invoice
		var invoice *InvoiceSettlementData
		if cached, ok := invoiceCache[invoiceNumber]; ok {
			invoice = cached
		} else {
			var invData InvoiceSettlementData
			err = tx.QueryRow(`
				SELECT sales_invoice_id, branch_id, outlet_id, amount
				FROM list_sales_invoice
				WHERE sales_invoice_number = ?
				LIMIT 1
			`, invoiceNumber).Scan(&invData.SalesInvoiceID, &invData.BranchID, &invData.OutletID, &invData.Amount)

			if err == sql.ErrNoRows {
				fmt.Printf("Invoice not found: %s\n", invoiceNumber)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying invoice: " + err.Error()
				goto FINISH
			}
			invoice = &invData
			invoiceCache[invoiceNumber] = invoice
		}

		// Get or cache region
		var region *RegionData
		if cached, ok := regionCache[invoice.BranchID]; ok {
			region = cached
		} else {
			var regData RegionData
			err = tx.QueryRow(`
				SELECT region_id
				FROM list_region
				WHERE region_purpose_id = 2 AND branch_id = ?
				LIMIT 1
			`, invoice.BranchID).Scan(&regData.RegionID)

			if err == sql.ErrNoRows {
				fmt.Printf("Region not found for branch: %d\n", invoice.BranchID)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying region: " + err.Error()
				goto FINISH
			}
			region = &regData
			regionCache[invoice.BranchID] = region
		}

		// Get collector (default to 1 if not found)
		collector := int64(1)
		err = tx.QueryRow(`
			SELECT ga.admin_id
			FROM rel_admin_region rar
			JOIN gemstone_admin ga ON ga.admin_id = rar.admin_id
			WHERE region_id = ? AND rar.is_active = 1 AND rar.is_exclusive = 0 AND ga.is_collector = 1
			LIMIT 1
		`, region.RegionID).Scan(&collector)
		if err == sql.ErrNoRows {
			collector = 1
		} else if err != nil {
			_ = tx.Rollback()
			resp.Message = "error querying collector: " + err.Error()
			goto FINISH
		}

		// Parse payment method
		paymentMethod := 1 // default cash
		if paymentMethodPtr != nil {
			pmStr := strings.ToLower(strings.TrimSpace(*paymentMethodPtr))
			if strings.Contains(pmStr, "cash") {
				paymentMethod = 1
			} else if strings.Contains(pmStr, "transfer") {
				paymentMethod = 2
			} else if strings.Contains(pmStr, "giro") {
				paymentMethod = 3
			}
		}

		// Handle GIRO payment - aggregate and continue
		if paymentMethod == 3 {
			if giroNumberPtr == nil {
				fmt.Println("giro number nil")
				continue
			}
			giroNumber := strings.TrimSpace(*giroNumberPtr)

			// Get or cache giro
			var giro *GiroData
			if cached, ok := giroCache[giroNumber]; ok {
				giro = cached
			} else {
				var gData GiroData
				err = tx.QueryRow(`
					SELECT giro_id, due_date
					FROM list_giro_check
					WHERE giro_number = ?
					LIMIT 1
				`, giroNumber).Scan(&gData.GiroID, &gData.DueDate)

				if err == sql.ErrNoRows {
					log.Printf("Missing Giro: %s\n", giroNumber)
					continue
				} else if err != nil {
					_ = tx.Rollback()
					resp.Message = "error querying giro: " + err.Error()
					goto FINISH
				}
				giro = &gData
				giroCache[giroNumber] = giro
			}

			// Insert rel_giro_invoice
			_, err = tx.Exec(`
				INSERT INTO rel_giro_invoice (giro_id, sales_invoice_id, amount)
				VALUES (?, ?, ?)
			`, giro.GiroID, invoice.SalesInvoiceID, settlementAmount)
			if err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting giro invoice: " + err.Error()
				goto FINISH
			}

			// Aggregate giro settlement data
			if _, exists := settlementGiroList[giro.GiroID]; !exists {
				settlementGiroList[giro.GiroID] = &SettlementGiroGroup{
					GiroID:        giro.GiroID,
					GiroNumber:    giroNumber,
					GiroDueDate:   giro.DueDate,
					PaymentMethod: paymentMethod,
					DTHDate:       dthDate.(string),
					Collector:     collector,
					BranchID:      invoice.BranchID,
					RegionID:      region.RegionID,
					OutletID:      invoice.OutletID,
					InvoiceList:   []GiroInvoiceItem{},
				}
			}

			group := settlementGiroList[giro.GiroID]
			group.TotalSettlementAmount += settlementAmount
			group.TotalGiroAmount += giroAmount
			group.InvoiceList = append(group.InvoiceList, GiroInvoiceItem{
				SalesInvoiceID:   invoice.SalesInvoiceID,
				SettlementAmount: settlementAmount,
				GiroAmount:       giroAmount,
			})
			fmt.Println("skip regular settlement for giro")
			continue // Skip regular settlement for giro
		}

		// Regular settlement (Cash/Transfer)
		// Generate draft_dth_number and dth_number (simplified - should use proper generator)
		draftDTHNumber := fmt.Sprintf("DRAFT-DTH-%d-%d", invoice.BranchID, time.Now().UnixNano())
		dthNumber := fmt.Sprintf("DTH-%d-%d", invoice.BranchID, time.Now().UnixNano())

		// INSERT list_debt_collection
		res, err := tx.Exec(`
			INSERT INTO list_debt_collection (
				debt_collection_draft_number, debt_collection_number, debt_collection_date,
				debt_collection_status_id, debt_collection_type_id, collector,
				branch_id, region_id, createdAt, createdBy, approvedAt, approvedBy
			) VALUES (?, ?, ?, 3, 1, ?, ?, ?, NOW(), ?, NOW(), ?)
		`, draftDTHNumber, dthNumber, dthDate, collector, invoice.BranchID, region.RegionID, *adminID, *adminID)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting debt collection: " + err.Error()
			goto FINISH
		}
		dthID, _ := res.LastInsertId()

		// INSERT rel_debt_collection_invoice
		_, err = tx.Exec(`
			INSERT INTO rel_debt_collection_invoice (debt_collection_id, outlet_id, invoice_id, amount_invoice)
			VALUES (?, ?, ?, ?)
		`, dthID, invoice.OutletID, invoice.SalesInvoiceID, settlementAmount)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting debt collection invoice: " + err.Error()
			goto FINISH
		}

		// Generate cashier receipt number
		cashierReceiptNumber := fmt.Sprintf("CR-%d-%d", invoice.BranchID, time.Now().UnixNano())

		// INSERT list_cashier_receipt
		res, err = tx.Exec(`
			INSERT INTO list_cashier_receipt (
				cashier_receipt_number, cashier_receipt_status_id, debt_collection_id,
				cash, giro, transfer, createdAt, createdBy
			) VALUES (?, 2, ?, ?, ?, ?, NOW(), ?)
		`, cashierReceiptNumber, dthID, cashAmount, giroAmount, transferAmount, *adminID)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting cashier receipt: " + err.Error()
			goto FINISH
		}
		cashierReceiptID, _ := res.LastInsertId()

		// Generate settlement numbers
		draftSettlementNumber := fmt.Sprintf("DRAFT-STL-%d-%d", invoice.BranchID, time.Now().UnixNano())
		settlementNumber := fmt.Sprintf("STL-%d-%d", invoice.BranchID, time.Now().UnixNano())

		// INSERT list_settlement
		res, err = tx.Exec(`
			INSERT INTO list_settlement (
				settlement_date, settlement_draft_number, settlement_number,
				debt_collection_id, cashier_receipt_id, settlement_status_id,
				branch_id, createdAt, createdBy
			) VALUES (?, ?, ?, ?, ?, 2, ?, NOW(), ?)
		`, dthDate, draftSettlementNumber, settlementNumber, dthID, cashierReceiptID, invoice.BranchID, *adminID)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting settlement: " + err.Error()
			goto FINISH
		}
		settlementID, _ := res.LastInsertId()

		// INSERT list_settlement_group
		giroNumberVal := sql.NullString{}
		giroDueDateVal := sql.NullString{}
		if giroNumberPtr != nil {
			giroNumberVal = sql.NullString{String: *giroNumberPtr, Valid: true}
		}

		res, err = tx.Exec(`
			INSERT INTO list_settlement_group (
				settlement_id, outlet_id, payment_method_id, settlement_amount,
				giro_number, giro_due_date
			) VALUES (?, ?, ?, ?, ?, ?)
		`, settlementID, invoice.OutletID, paymentMethod, settlementAmount, giroNumberVal, giroDueDateVal)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting settlement group: " + err.Error()
			goto FINISH
		}
		settlementGroupID, _ := res.LastInsertId()

		// INSERT rel_settle_invoice
		_, err = tx.Exec(`
			INSERT INTO rel_settle_invoice (
				sales_invoice_id, settlement_id, settlement_group_id,
				payment_amount, rounding_amount, outstanding_balance
			) VALUES (?, ?, ?, ?, 0, ?)
		`, invoice.SalesInvoiceID, settlementID, settlementGroupID, settlementAmount, invoice.Amount)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting settle invoice: " + err.Error()
			goto FINISH
		}

		insertedCount++
	}

	// Process aggregated GIRO settlements
	for _, giroGroup := range settlementGiroList {
		// Generate numbers
		draftDTHNumber := fmt.Sprintf("DRAFT-DTH-GIRO-%d-%d", giroGroup.BranchID, time.Now().UnixNano())
		dthNumber := fmt.Sprintf("DTH-GIRO-%d-%d", giroGroup.BranchID, time.Now().UnixNano())

		// INSERT list_debt_collection (status 3 for giro)
		res, err := tx.Exec(`
			INSERT INTO list_debt_collection (
				debt_collection_draft_number, debt_collection_number, debt_collection_date,
				debt_collection_status_id, debt_collection_type_id, collector,
				branch_id, region_id, createdAt, createdBy, approvedAt, approvedBy
			) VALUES (?, ?, ?, 3, 1, ?, ?, ?, NOW(), ?, NOW(), ?)
		`, draftDTHNumber, dthNumber, giroGroup.DTHDate, giroGroup.Collector, giroGroup.BranchID, giroGroup.RegionID, *adminID, *adminID)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting giro debt collection: " + err.Error()
			goto FINISH
		}
		dthID, _ := res.LastInsertId()

		// INSERT rel_debt_collection_invoice for each invoice in group
		for _, invItem := range giroGroup.InvoiceList {
			_, err = tx.Exec(`
				INSERT INTO rel_debt_collection_invoice (debt_collection_id, outlet_id, invoice_id, amount_invoice)
				VALUES (?, ?, ?, ?)
			`, dthID, giroGroup.OutletID, invItem.SalesInvoiceID, invItem.SettlementAmount)
			if err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting giro debt collection invoice: " + err.Error()
				goto FINISH
			}
		}

		// Generate cashier receipt number
		cashierReceiptNumber := fmt.Sprintf("CR-GIRO-%d-%d", giroGroup.BranchID, time.Now().UnixNano())

		// INSERT list_cashier_receipt (giro only)
		res, err = tx.Exec(`
			INSERT INTO list_cashier_receipt (
				cashier_receipt_number, cashier_receipt_status_id, debt_collection_id,
				cash, giro, transfer, createdAt, createdBy
			) VALUES (?, 2, ?, 0, 0, ?, NOW(), ?)
		`, cashierReceiptNumber, dthID, giroGroup.TotalGiroAmount, *adminID)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting giro cashier receipt: " + err.Error()
			goto FINISH
		}
		cashierReceiptID, _ := res.LastInsertId()

		// Generate settlement numbers
		draftSettlementNumber := fmt.Sprintf("DRAFT-STL-GIRO-%d-%d", giroGroup.BranchID, time.Now().UnixNano())
		settlementNumber := fmt.Sprintf("STL-GIRO-%d-%d", giroGroup.BranchID, time.Now().UnixNano())

		// INSERT list_settlement
		res, err = tx.Exec(`
			INSERT INTO list_settlement (
				settlement_date, settlement_draft_number, settlement_number,
				debt_collection_id, cashier_receipt_id, settlement_status_id,
				branch_id, createdAt, createdBy
			) VALUES (?, ?, ?, ?, ?, 2, ?, NOW(), ?)
		`, giroGroup.DTHDate, draftSettlementNumber, settlementNumber, dthID, cashierReceiptID, giroGroup.BranchID, *adminID)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting giro settlement: " + err.Error()
			goto FINISH
		}
		settlementID, _ := res.LastInsertId()

		// INSERT list_settlement_group
		layoutIn := time.RFC3339           // format dari Go, contoh: 2025-10-18T00:00:00Z
		layoutOut := "2006-01-02 15:04:05" // format MySQL

		var formattedDueDate interface{} = nil
		if giroGroup.GiroDueDate != "" {
			t, err := time.Parse(layoutIn, giroGroup.GiroDueDate)
			if err != nil {
				_ = tx.Rollback()
				resp.Message = "invalid giro due date format: " + err.Error()
				goto FINISH
			}
			formattedDueDate = t.Format(layoutOut)
		}

		res, err = tx.Exec(`
			INSERT INTO list_settlement_group (
				settlement_id, outlet_id, payment_method_id, settlement_amount,
				giro_number, giro_due_date
			) VALUES (?, ?, ?, ?, ?, ?)
		`, settlementID, giroGroup.OutletID, giroGroup.PaymentMethod, giroGroup.TotalSettlementAmount, giroGroup.GiroNumber, formattedDueDate)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting giro settlement group: " + err.Error()
			goto FINISH
		}
		settlementGroupID, _ := res.LastInsertId()

		// INSERT rel_settle_invoice for each invoice in group
		for _, invItem := range giroGroup.InvoiceList {
			_, err = tx.Exec(`
				INSERT INTO rel_settle_invoice (
					sales_invoice_id, settlement_id, settlement_group_id,
					payment_amount, rounding_amount, outstanding_balance
				) VALUES (?, ?, ?, ?, 0, ?)
			`, invItem.SalesInvoiceID, settlementID, settlementGroupID, invItem.GiroAmount, invItem.SettlementAmount)
			if err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting giro settle invoice: " + err.Error()
				goto FINISH
			}
		}

		// UPDATE list_giro_check with settlement_id
		_, err = tx.Exec(`
			UPDATE list_giro_check SET settlement_id = ? WHERE giro_id = ?
		`, settlementID, giroGroup.GiroID)
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error updating giro check: " + err.Error()
			goto FINISH
		}

		insertedCount++
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		resp.Message = "db commit error: " + err.Error()
		goto FINISH
	}

	resp.Success = true
	resp.Message = "Import Settlement Success"
	resp.MessageDetail = fmt.Sprintf("Total %d settlements inserted. Execution Time: %.4fs", insertedCount, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import settlement complete: %d settlements, time=%.4fs\n", insertedCount, time.Since(start).Seconds())
}

// Helper structs
type InvoiceSettlementData struct {
	SalesInvoiceID int64
	BranchID       int64
	OutletID       int64
	Amount         float64
}

type RegionData struct {
	RegionID int64
}

type GiroData struct {
	GiroID  int64
	DueDate string
}
