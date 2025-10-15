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

// helper types for caching DB rows minimal fields (adapt if you have more)
type Branch struct {
	ID   int64
	Name string
}
type Outlet struct {
	ID       int64
	Name     string
	TopValue string
}
type Region struct {
	ID   int64
	Code string
	Name string
}
type Principal struct {
	ID int64
}
type Admin struct {
	ID int64
}
type SalesSource struct {
	ID int64
}
type ReturnInvoice struct {
	ID int64
}

func RunImportSalesInvoiceCmd(args []string) {
	fs := flag.NewFlagSet("invoice", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/invoice.xlsx", "path to xlsx file")
	dsn := fs.String("dsn", "", "mysql DSN, e.g. user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true")
	adminID := fs.Int("admin-id", 1, "createdBy admin id")
	batchSize := fs.Int("batch", 500, "batch insert size")
	logID := fs.String("log-id", "", "optional log_id to update activity on success")
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

	// open excel file
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

	rowsIter, err := f.Rows(sheet)
	if err != nil {
		resp.Message = "error reading sheet rows: " + err.Error()
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}
	defer rowsIter.Close()

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

	// fixed createdAt per your request
	createdAt := "2025-09-29 00:00:00"

	// caches
	branchCache := map[string]*Branch{} // branchCode -> Branch
	outletCache := map[string]*Outlet{} // outletCode -> Outlet
	regionCache := map[string]*Region{} // uniqueKey -> Region
	principalCache := map[string]*Principal{}
	adminCache := map[string]*Admin{}
	sourceCache := map[string]*SalesSource{}
	returnInvoiceCache := map[string]*ReturnInvoice{}
	invoiceExistsCache := map[string]bool{}

	// batch builders: we will batch-insert into 3 tables
	orderCols := []string{
		"outlet_id", "division_id", "branch_id", "branch_billing_id",
		"sales_date", "sales_number", "sales_order_status_id",
		"payment_method", "sales_source_id", "sales_type_id",
		"salesman_id", "region_id", "principal_id",
		"stamp_duty", "is_ecatalogue", "term_days",
		"amount", "ppn", "cash_discount",
		"createdAt", "createdBy", "is_legacy",
	}
	invoiceCols := []string{
		"outlet_id", "division_id", "branch_id", "branch_billing_id",
		"sales_invoice_date", "sales_invoice_number", "internal_note",
		"sales_invoice_status_id", "payment_method", "sales_source_id",
		"sales_invoice_type_id", "salesman_id", "region_id",
		"principal_id", "stamp_duty", "is_ecatalogue",
		"is_b2b", "term_days", "amount", "ppn", "cash_discount",
		"is_return_invoice", "createdAt", "createdBy", "is_legacy",
	}
	skbCols := []string{
		"skb_number", "skb_date", "skb_status_id", "skb_type_id",
		"issuer_warehouse_id", "issuer_type_id", "issuer_id", "issuer",
		"destination_type_id", "destination_id", "destination",
		"is_complete", "createdAt", "createdBy", "division_id", "approvedAt", "approvedBy",
		"pharmacist_verified_by", "pharmacist_verified_at",
	}

	batchOrderRows := [][]interface{}{}
	batchInvoiceRows := [][]interface{}{}
	batchSkbRows := [][]interface{}{}

	uniqueInvoiceList := map[string]bool{}
	returnInvoiceStb := []struct {
		InvoiceNumber   string
		ReturnInvoiceID int64
	}{}

	// iterate rows
	rowIndex := 0
	insertedCount := 0
	for rowsIter.Next() {
		rowIndex++
		cols, err := rowsIter.Columns()
		if err != nil {
			_ = tx.Rollback()
			resp.Message = "error reading row: " + err.Error()
			goto FINISH
		}
		// skip header
		if rowIndex == 1 {
			fmt.Println("Skip Header")
			continue
		}

		// normalize length so we can index safely (like PHP code expects many cols)
		// ensure at least, say, 20 columns (we will index up to maybe 17); expand if needed
		for len(cols) < 30 {
			cols = append(cols, "")
		}

		// helper get col pointer trimmed
		getCol := func(idx int) *string {
			if idx < len(cols) {
				return checkIsTrueEmpty(cols[idx])
			}
			return nil
		}

		// invoice_date column 0 may be excel date or string
		var invoiceDate string
		if p := getCol(0); p != nil {
			invoiceDate = parseExcelDate(*p)
		} else {
			// if empty -> skip
			// fmt.Println("Invoice date nil at row: ", rowIndex)
			fmt.Println("invoice date dilewati")
			continue
		}

		// invoice_number
		invoiceNumberPtr := getCol(1)
		if invoiceNumberPtr == nil || *invoiceNumberPtr == "" {
			// no invoice number -> skip
			fmt.Println("Invoice number nil")
			continue
		}
		invoiceNumber := *invoiceNumberPtr

		if invoiceNumber == "Freetext" {
			continue
		}

		var existsInv int
		errInv := tx.QueryRow(`SELECT 1 FROM list_sales_invoice WHERE sales_invoice_number = ? LIMIT 1`, invoiceNumber).Scan(&existsInv)
		if errInv == nil {
			fmt.Println("Invoice sudah ada, skip: ", invoiceNumber)
			continue
		}

		// check if invoice already exists (avoid re-querying DB many times)
		if _, ok := invoiceExistsCache[invoiceNumber]; !ok {
			var exists int
			err := tx.QueryRow("SELECT 1 FROM list_sales_invoice WHERE sales_invoice_number = ? LIMIT 1", invoiceNumber).Scan(&exists)
			if err != nil && err != sql.ErrNoRows {
				_ = tx.Rollback()
				resp.Message = "db error checking invoice existence: " + err.Error()
				goto FINISH
			}
			invoiceExistsCache[invoiceNumber] = (err == nil)
		}
		if invoiceExistsCache[invoiceNumber] {
			// skip existing
			fmt.Println("Invoice exist cache: ", invoiceNumber)
			continue
		}

		// note
		notePtr := getCol(2)
		note := ""
		if notePtr != nil {
			note = *notePtr
		}

		// branch (col 3)
		branchCodePtr := getCol(3)
		if branchCodePtr == nil || *branchCodePtr == "" {
			log.Printf("Missing branch code at row %d\n", rowIndex)
			fmt.Printf("Missing branch code at row %d\n", rowIndex)
			continue
		}
		branchCode := *branchCodePtr

		// cached branch lookup
		var branch *Branch
		if b, ok := branchCache[branchCode]; ok {
			branch = b
		} else {
			var bid int64
			var bname sql.NullString
			err := tx.QueryRow("SELECT branch_id, branch_name FROM list_branch WHERE branch_code = ? LIMIT 1", branchCode).Scan(&bid, &bname)
			if err == sql.ErrNoRows {
				log.Printf("Missing branch: %s (row %d)\n", branchCode, rowIndex)
				fmt.Println("Missing branch: ", branchCode)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "db error querying branch: " + err.Error()
				goto FINISH
			}
			nb := &Branch{ID: bid}
			if bname.Valid {
				nb.Name = bname.String
			}
			branchCache[branchCode] = nb
			branch = nb
		}

		// outlet (col 4)
		outletCodePtr := getCol(4)
		if outletCodePtr == nil || *outletCodePtr == "" {
			log.Printf("Missing outlet code at row %d\n", rowIndex)
			fmt.Println("Missing outlet: ", *outletCodePtr)
			continue
		}
		outletCode := *outletCodePtr

		var outlet *Outlet
		if o, ok := outletCache[outletCode]; ok {
			outlet = o
		} else {
			var oid int64
			var oname, oTopValue sql.NullString
			err := tx.QueryRow("SELECT outlet_id, outlet_name, top_value FROM list_outlet WHERE outlet_code = ? LIMIT 1", outletCode).Scan(&oid, &oname, &oTopValue)
			if err == sql.ErrNoRows {
				log.Printf("Missing outlet: %s (row %d)\n", outletCode, rowIndex)
				fmt.Println("Missing outlet: ", outletCode)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "db error querying outlet: " + err.Error()
				goto FINISH
			}
			no := &Outlet{ID: oid}
			if oname.Valid {
				no.Name = oname.String
			}
			if oTopValue.Valid {
				no.TopValue = oTopValue.String
			}
			outletCache[outletCode] = no
			outlet = no
		}

		// division (col 5)
		divPtr := getCol(5)
		divisionID := 3
		if divPtr != nil {
			if strings.EqualFold(*divPtr, "Pharmacy") {
				divisionID = 1
			} else if strings.EqualFold(*divPtr, "Hoslab") {
				divisionID = 2
			}
		}

		// principal B2B (col 6)
		principalNamePtr := getCol(6)
		var principalID sql.NullInt64
		isB2B := 0
		if principalNamePtr != nil && *principalNamePtr != "" {
			pn := *principalNamePtr
			if p, ok := principalCache[pn]; ok {
				principalID = sql.NullInt64{Int64: p.ID, Valid: true}
				isB2B = 1
			} else {
				var pid int64
				err := tx.QueryRow("SELECT principal_id FROM list_principal WHERE principal_name = ? LIMIT 1", pn).Scan(&pid)
				if err == sql.ErrNoRows {
					// not found -> leave null and isB2B = 0
				} else if err != nil {
					_ = tx.Rollback()
					resp.Message = "db error querying principal: " + err.Error()
					goto FINISH
				} else {
					principalCache[pn] = &Principal{ID: pid}
					principalID = sql.NullInt64{Int64: pid, Valid: true}
					isB2B = 1
				}
			}
		}

		// payment method (col 7) and source (col 8)
		paymentMethodPtr := getCol(7)
		paymentMethod := ""
		if paymentMethodPtr != nil {
			paymentMethod = strings.Title(strings.ToLower(*paymentMethodPtr))
		}
		sourcePtr := getCol(8)
		var sourceID int64
		if sourcePtr == nil || *sourcePtr == "" {
			log.Printf("Missing sales source at row %d\n", rowIndex)
			fmt.Println("Missing sales source: ", *sourcePtr)
			continue
		}
		srcKey := strings.ToLower(*sourcePtr)
		if s, ok := sourceCache[srcKey]; ok {
			sourceID = s.ID
		} else {
			var sid int64
			err := tx.QueryRow("SELECT source_id FROM list_sales_source WHERE source_name = ? LIMIT 1", srcKey).Scan(&sid)
			if err == sql.ErrNoRows {
				log.Printf("Missing Sales Source: %s (row %d)\n", srcKey, rowIndex)
				fmt.Println("Missing sales source: ", srcKey)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "db error querying sales source: " + err.Error()
				goto FINISH
			}
			sourceCache[srcKey] = &SalesSource{ID: sid}
			sourceID = sid
		}

		// region (col 9) -> cached by unique_id = region_code + "-1-" + branch_id
		regionCodePtr := getCol(9)
		regionID := int64(0)
		if regionCodePtr != nil && *regionCodePtr != "" {
			uniqueID := fmt.Sprintf("%s-1-%d", *regionCodePtr, branch.ID)
			if r, ok := regionCache[uniqueID]; ok {
				regionID = r.ID
			} else {
				var rid int64
				// try get region by code & branch & purpose (1)
				err := tx.QueryRow("SELECT region_id FROM list_region WHERE region_code = ? AND branch_id = ? AND region_purpose_id = 1 LIMIT 1", *regionCodePtr, branch.ID).Scan(&rid)
				if err == sql.ErrNoRows {
					// create region
					res, errIns := tx.Exec(`INSERT INTO list_region (region_name, region_code, branch_id, region_type_id, region_status_id, region_purpose_id, createdAt, createdBy)
                        VALUES (?, ?, ?, 1, 2, 1, NOW(), ?)`, *regionCodePtr, *regionCodePtr, branch.ID, *adminID)
					if errIns != nil {
						_ = tx.Rollback()
						resp.Message = "error inserting region: " + errIns.Error()
						goto FINISH
					}
					lastID, _ := res.LastInsertId()
					rid = lastID
					log.Printf("Inserted missing region %s -> id %d\n", *regionCodePtr, rid)
				} else if err != nil {
					_ = tx.Rollback()
					resp.Message = "db error querying region: " + err.Error()
					goto FINISH
				}
				regionCache[uniqueID] = &Region{ID: rid}
				regionID = rid
			}
		} else {
			// region code empty - skip like PHP did (or continue) -> PHP created region and then continue
			log.Printf("Empty region at row %d - skipping\n", rowIndex)
			fmt.Println("Empty region: ", *regionCodePtr)
			continue
		}

		// admin / salesman (col 10) - insert if missing
		adminNamePtr := getCol(10)
		var salesmanID int64
		if adminNamePtr == nil || *adminNamePtr == "" {
			// fallback to provided adminID
			salesmanID = int64(*adminID)
		} else {
			adminName := *adminNamePtr
			if a, ok := adminCache[adminName]; ok {
				salesmanID = a.ID
			} else {
				var aid int64
				err := tx.QueryRow("SELECT admin_id FROM gemstone_admin WHERE admin_name = ? LIMIT 1", adminName).Scan(&aid)
				if err == sql.ErrNoRows {
					// insert new admin with password "admin" hashed? we will insert with static hash placeholder
					// to avoid adding bcrypt dependency here, insert with a placeholder password (you can change)
					hashedPass := "$2y$10$BpYtQGwQSSTM79aUVJdW7.gwdOCJ.cY29g.sc1KS3qusyU8U4eHFu" // replace if you want bcrypt
					createdAt := time.Now().Format("2006-01-02 15:04:05")
					res, errIns := tx.Exec("INSERT INTO gemstone_admin (admin_name, admin_fullname, admin_tier_id, password, admin_status, last_active) VALUES (?, ?, 30, ?, 1, ?)",
						adminName, adminName, hashedPass, createdAt)
					if errIns != nil {
						// if insert fails, fallback to provided adminID
						log.Printf("Failed insert admin %s: %v - using adminID fallback\n", adminName, errIns)
						salesmanID = int64(*adminID)
					} else {
						last, _ := res.LastInsertId()
						aid = last
						salesmanID = aid
					}
				} else if err != nil {
					_ = tx.Rollback()
					resp.Message = "db error querying admin: " + err.Error()
					goto FINISH
				} else {
					salesmanID = aid
				}
				adminCache[adminName] = &Admin{ID: salesmanID}
			}
		}

		// stamp duty (col 11)
		stampDuty := 0
		if p := getCol(11); p != nil {
			if strings.EqualFold(*p, "ya") {
				stampDuty = 1
			}
		}

		// amount (col 12) - keep as string numeric; PHP used check_is_true_empty so allow empty => 0
		amount := "0"
		if p := getCol(12); p != nil && *p != "" {
			amount = denormalizeNumber(p) // denormalizeNumber returns string without thousands separator in your earlier code
		}

		// ppn default 11 (php)
		ppn := 11

		// discount (col 14)
		discount := "0"
		if p := getCol(14); p != nil {
			d := strings.TrimSpace(*p)
			if d == "" || d == "-" {
				discount = "0"
			} else {
				discount = d
			}
		}

		// sales_type (col 15)
		salesTypeRaw := ""
		if p := getCol(15); p != nil {
			salesTypeRaw = *p
		}
		var salesTypeID int
		var skbTypeID int
		if strings.EqualFold(salesTypeRaw, "reguler") || salesTypeRaw == "1" {
			salesTypeID = 1
			skbTypeID = 6 // placeholder for SKBType::PENJUALAN (use actual constant if you have)
		} else if strings.EqualFold(salesTypeRaw, "konsinyasi") || salesTypeRaw == "2" {
			salesTypeID = 2
			skbTypeID = 5 // SKBType::KONSINYASI_OUTLET
		} else {
			salesTypeID = 3
			skbTypeID = 11 // SKBType::TENDER_OUTLET
		}

		// return invoice number (col 16)
		returnInvoicePtr := getCol(16)
		if returnInvoicePtr != nil && *returnInvoicePtr != "" {
			rn := *returnInvoicePtr
			if _, ok := returnInvoiceCache[rn]; !ok {
				var rid int64
				err := tx.QueryRow("SELECT return_invoice_id FROM list_invoice_return WHERE return_number = ? LIMIT 1", rn).Scan(&rid)
				if err == sql.ErrNoRows {
					// not found -> skip caching
					returnInvoiceCache[rn] = nil
				} else if err != nil {
					_ = tx.Rollback()
					resp.Message = "db error querying return invoice: " + err.Error()
					goto FINISH
				} else {
					returnInvoiceCache[rn] = &ReturnInvoice{ID: rid}
				}
			}
			if ri := returnInvoiceCache[rn]; ri != nil {
				returnInvoiceStb = append(returnInvoiceStb, struct {
					InvoiceNumber   string
					ReturnInvoiceID int64
				}{InvoiceNumber: invoiceNumber, ReturnInvoiceID: ri.ID})
			}
		}

		// issuer warehouse (query) -> get warehouse_id by branch_id and type 1
		var issuerWarehouseID int64
		err = tx.QueryRow("SELECT warehouse_id FROM list_warehouse WHERE branch_id = ? AND warehouse_type_id = 1 LIMIT 1", branch.ID).Scan(&issuerWarehouseID)
		if err != nil {
			// if no warehouse, we will attempt to create default warehouse for branch
			if err == sql.ErrNoRows {
				// try create warehouse named "Default"
				res, errIns := tx.Exec("INSERT INTO list_warehouse (warehouse_name, branch_id, createdAt, createdBy) VALUES (?, ?, ?, ?)",
					"Default", branch.ID, createdAt, *adminID)
				if errIns != nil {
					_ = tx.Rollback()
					resp.Message = "error creating default warehouse: " + errIns.Error()
					goto FINISH
				}
				lastID, _ := res.LastInsertId()
				issuerWarehouseID = lastID
			} else {
				_ = tx.Rollback()
				resp.Message = "db error querying issuer warehouse: " + err.Error()
				goto FINISH
			}
		}

		// Build SQL value pieces (we will use buildMultiInsert to param) - preserve PHP order
		// list_sales_order row:
		// orderCols := [outlet_id, division_id, branch_id, branch_billing_id, sales_date, sales_number, sales_order_status_id,
		// payment_method, sales_source_id, sales_type_id, salesman_id, region_id, principal_id, stamp_duty, is_ecatalogue,
		// term_days, amount, ppn, cash_discount, createdAt, createdBy, is_legacy]

		// convert numeric strings to appropriate types as needed: PHP used db->escape on many values (strings). We'll keep types as interface{}
		// For simplicity, we will insert amount and discount as numeric strings (MySQL will cast) — you may change to float64 if desired.

		// skip duplicates in same file processing
		if _, ok := uniqueInvoiceList[invoiceNumber]; !ok {
			uniqueInvoiceList[invoiceNumber] = true

			// build order row
			orderRow := []interface{}{
				outlet.ID,     // outlet_id
				divisionID,    // division_id
				branch.ID,     // branch_id
				branch.ID,     // branch_billing_id
				invoiceDate,   // sales_date
				invoiceNumber, // sales_number
				3,             // SalesOrderStatus::SELESAI (?) PHP used constant: use 3 or  something; adjust if needed
				paymentMethod, // payment_method
				sourceID,      // sales_source_id
				salesTypeID,   // sales_type_id
				salesmanID,    // salesman_id
				regionID,      // region_id
				nil,           // principal_id -> will pass nil or principalID.Int64
				stampDuty,     // stamp_duty
				0,             // is_ecatalogue
				0,             // term_days
				amount,        // amount
				ppn,           // ppn
				discount,      // cash_discount
				createdAt,     // createdAt
				*adminID,      // createdBy
				1,             // is_legacy
			}
			// principal
			if principalID.Valid {
				orderRow[12] = principalID.Int64
			} else {
				orderRow[12] = nil
			}
			batchOrderRows = append(batchOrderRows, orderRow)

			// invoice row
			invRow := []interface{}{
				outlet.ID,
				divisionID,
				branch.ID,
				branch.ID,
				invoiceDate,
				invoiceNumber,
				note,
				3, // SalesInvoiceStatus::SELESAI - set 3 or adjust
				paymentMethod,
				sourceID,
				salesTypeID,
				salesmanID,
				regionID,
				nil, // principal id
				stampDuty,
				0, // is_ecatalogue
				isB2B,
				0, // term_days
				amount,
				ppn,
				discount,
				0, // is_return_invoice
				createdAt,
				*adminID,
				1,
			}
			if principalID.Valid {
				invRow[13] = principalID.Int64
			}
			batchInvoiceRows = append(batchInvoiceRows, invRow)

			// skb row
			skbRow := []interface{}{
				invoiceNumber,
				invoiceDate,
				4, // skb_status_id
				skbTypeID,
				issuerWarehouseID,
				1,         // issuer_type_id
				branch.ID, // issuer_id
				branch.Name,
				3,           // destination_type_id
				outlet.ID,   // destination_id
				outlet.Name, // destination
				1,           // is_complete
				createdAt,
				*adminID,
				divisionID,
				createdAt,
				*adminID,
				*adminID,
				createdAt,
			}
			batchSkbRows = append(batchSkbRows, skbRow)
		}

		// flush per batch size
		if len(batchOrderRows) >= *batchSize {
			// insert orders
			baseOrder := "INSERT INTO `list_sales_order`"
			qOrder, argsOrder := buildMultiInsert(baseOrder, orderCols, batchOrderRows)
			if _, err := tx.Exec(qOrder, argsOrder...); err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting batch orders: " + err.Error()
				goto FINISH
			}

			batchOrderRows = [][]interface{}{}

			// insert invoices
			if len(batchInvoiceRows) > 0 {
				baseInv := "INSERT INTO `list_sales_invoice`"
				qInv, argsInv := buildMultiInsert(baseInv, invoiceCols, batchInvoiceRows)
				if _, err := tx.Exec(qInv, argsInv...); err != nil {
					_ = tx.Rollback()
					resp.Message = "error inserting batch invoices: " + err.Error()
					goto FINISH
				}
				insertedCount += len(batchInvoiceRows)
				batchInvoiceRows = [][]interface{}{}
			}

			// insert skbs
			if len(batchSkbRows) > 0 {
				baseSkb := "INSERT INTO `list_skb`"
				qSkb, argsSkb := buildMultiInsert(baseSkb, skbCols, batchSkbRows)
				if _, err := tx.Exec(qSkb, argsSkb...); err != nil {
					_ = tx.Rollback()
					resp.Message = "error inserting batch skbs: " + err.Error()
					goto FINISH
				}
				batchSkbRows = [][]interface{}{}
			}
		}
	} // end rows iteration

	// flush remaining
	if len(batchOrderRows) > 0 {
		baseOrder := "INSERT INTO `list_sales_order`"
		qOrder, argsOrder := buildMultiInsert(baseOrder, orderCols, batchOrderRows)
		if _, err := tx.Exec(qOrder, argsOrder...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final orders: " + err.Error()
			goto FINISH
		}
	}
	if len(batchInvoiceRows) > 0 {
		baseInv := "INSERT INTO `list_sales_invoice`"
		qInv, argsInv := buildMultiInsert(baseInv, invoiceCols, batchInvoiceRows)
		if _, err := tx.Exec(qInv, argsInv...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final invoices: " + err.Error()
			goto FINISH
		}
		insertedCount += len(batchInvoiceRows)
	}
	if len(batchSkbRows) > 0 {
		baseSkb := "INSERT INTO `list_skb`"
		qSkb, argsSkb := buildMultiInsert(baseSkb, skbCols, batchSkbRows)
		if _, err := tx.Exec(qSkb, argsSkb...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final skbs: " + err.Error()
			goto FINISH
		}
	}

	// handle return_invoice_stb updates
	if len(returnInvoiceStb) > 0 {
		invoiceCache := map[string]int64{}
		for _, item := range returnInvoiceStb {
			// lookup invoice just inserted by sales_invoice_number
			if _, ok := invoiceCache[item.InvoiceNumber]; !ok {
				var sid int64
				err := tx.QueryRow("SELECT sales_invoice_id FROM list_sales_invoice WHERE sales_invoice_number = ? LIMIT 1", item.InvoiceNumber).Scan(&sid)
				if err == nil {
					invoiceCache[item.InvoiceNumber] = sid
				} else {
					// not found - skip
					invoiceCache[item.InvoiceNumber] = 0
				}
			}
			if invoiceCache[item.InvoiceNumber] == 0 {
				fmt.Println("Invoice cache: ", item.InvoiceNumber)
				continue
			}
			// update rel_return_invoice_stb
			if _, err := tx.Exec("UPDATE rel_return_invoice_stb SET reference_id = ? WHERE return_invoice_id = ? AND reference_id IS NULL",
				invoiceCache[item.InvoiceNumber], item.ReturnInvoiceID); err != nil {
				log.Printf("warning: failed update rel_return_invoice_stb for return_invoice_id=%d: %v\n", item.ReturnInvoiceID, err)
			}
			// update list_sales_invoice -> mark as return invoice
			if _, err := tx.Exec("UPDATE list_sales_invoice SET is_return_invoice = 1 WHERE sales_invoice_id = ?", invoiceCache[item.InvoiceNumber]); err != nil {
				log.Printf("warning: failed update list_sales_invoice is_return_invoice for id=%d: %v\n", invoiceCache[item.InvoiceNumber], err)
			}
		}
	}

	// commit or rollback
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		resp.Message = "db commit error: " + err.Error()
		goto FINISH
	}

	// update activity if logID provided
	if *logID != "" {
		tx2, err := db.Begin()
		if err == nil {
			_ = updateActivity(tx2, *logID, "IMPORT DATA SALES INVOICE", "sales/view_invoice_list", "{}")
			_ = tx2.Commit()
		}
	}

	resp.Success = true
	resp.Message = "Import Sales Invoice Success"
	resp.MessageDetail = fmt.Sprintf("Total %d rows inserted. Execution Time : %.4f seconds", insertedCount, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import_sales_invoice complete: time=%.4fs\n", time.Since(start).Seconds())
}
