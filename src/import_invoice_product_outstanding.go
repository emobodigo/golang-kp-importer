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

func RunImportSalesInvoiceProductOutstandingCmd(args []string) {
	fs := flag.NewFlagSet("invoice-product", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/invoice_product.xlsx", "path to xlsx file")
	dsn := fs.String("dsn", "", "mysql DSN, e.g. user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true")
	adminID := fs.Int("admin-id", 1, "createdBy admin id")
	batchSize := fs.Int("batch", 500, "batch size for inserts (for parity)")
	sheetName := fs.String("sheet", "", "sheet name (optional)")
	fs.Parse(args)

	start := time.Now()
	resp := Response{Success: false}
	// messageDetail := ""

	exitWith := func(msg string) {
		resp.Message = msg
		out, _ := json.Marshal(resp)
		fmt.Println(string(out))
		os.Exit(1)
	}

	if *dsn == "" {
		exitWith("dsn is required")
	}
	if _, err := os.Stat(*filePath); err != nil {
		exitWith(fmt.Sprintf("file not found: %s", *filePath))
	}

	f, err := excelize.OpenFile(*filePath)
	if err != nil {
		exitWith("error opening file: " + err.Error())
	}
	defer f.Close()

	sheet := *sheetName
	if sheet == "" {
		sheet = f.GetSheetName(0)
		if sheet == "" {
			exitWith("no sheet found")
		}
	}

	rows, err := f.GetRows(sheet)
	if err != nil {
		exitWith("error reading sheet rows: " + err.Error())
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		exitWith("db open error: " + err.Error())
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		exitWith("db begin error: " + err.Error())
	}

	// prepare statements
	stmtOrder, err := tx.Prepare(`
		INSERT INTO rel_sales_order_item
		(sales_order_id, product_id, quoted_price, discount_value, discount_routine_value, discount_program_value,
		 discount_routine_branch, discount_program_branch, dpp, unit, qty, qty_extra, temp_iteration)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 0, 2)
	`)
	if err != nil {
		_ = tx.Rollback()
		exitWith("prepare stmt_order failed: " + err.Error())
	}
	defer stmtOrder.Close()

	stmtOrderExtra, err := tx.Prepare(`
		INSERT INTO rel_sales_order_item
		(sales_order_id, product_id, quoted_price, discount_value, discount_routine_value, discount_program_value,
		 discount_routine_branch, discount_program_branch, dpp, unit, qty, qty_extra, group_id, temp_iteration)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 1, 0, ?, ?, 2)
	`)
	if err != nil {
		_ = tx.Rollback()
		exitWith("prepare stmt_order_extra failed: " + err.Error())
	}
	defer stmtOrderExtra.Close()

	stmtInvoice, err := tx.Prepare(`
		INSERT INTO rel_sales_invoice_item
		(sales_invoice_id, product_id, salesman_id, quoted_price, batch_number, discount_value,
		 discount_routine_value, discount_program_value, discount_routine_branch, discount_program_branch,
		 dpp, unit, qty, qty_extra, skb_id, temp_iteration)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 0, ?, 2)
	`)
	if err != nil {
		_ = tx.Rollback()
		exitWith("prepare stmt_invoice failed: " + err.Error())
	}
	defer stmtInvoice.Close()

	stmtInvoiceExtra, err := tx.Prepare(`
		INSERT INTO rel_sales_invoice_item
		(sales_invoice_id, product_id, salesman_id, quoted_price, batch_number, discount_value,
		 discount_routine_value, discount_program_value, discount_routine_branch, discount_program_branch,
		 dpp, unit, qty, qty_extra, group_id, skb_id, temp_iteration)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, 0, ?, ?, ?, 2)
	`)
	if err != nil {
		_ = tx.Rollback()
		exitWith("prepare stmt_invoice_extra failed: " + err.Error())
	}
	defer stmtInvoiceExtra.Close()

	stmtSkb, err := tx.Prepare(`
		INSERT INTO rel_skb_item
		(skb_id, product_id, unit, qty, quoted_price, batch_number, expired_date, reference_type_id, reference_id)
		VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		_ = tx.Rollback()
		exitWith("prepare stmt_skb failed: " + err.Error())
	}
	defer stmtSkb.Close()

	stmtSkbExtra, err := tx.Prepare(`
		INSERT INTO rel_skb_item
		(skb_id, product_id, unit, qty, quoted_price, batch_number, expired_date, reference_type_id, reference_id, is_extra)
		VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, 1)
	`)
	if err != nil {
		_ = tx.Rollback()
		exitWith("prepare stmt_skb_extra failed: " + err.Error())
	}
	defer stmtSkbExtra.Close()

	// caches
	orderCache := map[string]int64{}
	invoiceCache := map[string]struct {
		ID       int64
		Salesman int64
		TypeInv  int64
		InvDate  string
	}{}
	skbCache := map[string]int64{}
	productCache := make(map[string]ProductCacheData)
	invoiceSKBLinked := map[string]bool{}

	insertedCount := 0

	for r := 1; r < len(rows); r++ {
		cols := rows[r]

		if len(cols) < 6 {
			fmt.Println("coloum lebih kecil dari 6")
			continue
		}

		getCol := func(i int) string {
			if i < len(cols) {
				return strings.TrimSpace(cols[i])
			}
			return ""
		}

		invoiceNumber := getCol(0)
		if invoiceNumber == "" {
			fmt.Println("invoice number kosong")
			continue
		}

		// order
		orderID, ok := orderCache[invoiceNumber]
		if !ok {
			err := tx.QueryRow("SELECT sales_order_id FROM list_sales_order WHERE sales_number = ? LIMIT 1", invoiceNumber).Scan(&orderID)
			if err == sql.ErrNoRows {
				log.Printf("missing order: %s", invoiceNumber)
				fmt.Println("missing order: ", invoiceNumber)
				continue
			}
			if err != nil {
				_ = tx.Rollback()
				exitWith("error querying order: " + err.Error())
			}
			orderCache[invoiceNumber] = orderID
		}

		// invoice
		invData, ok := invoiceCache[invoiceNumber]
		if !ok {
			var siID, salesmanID, typeInv sql.NullInt64
			var invDate string
			err := tx.QueryRow("SELECT sales_invoice_id, salesman_id, sales_invoice_type_id, sales_invoice_date FROM list_sales_invoice WHERE sales_invoice_number = ? LIMIT 1", invoiceNumber).
				Scan(&siID, &salesmanID, &typeInv, &invDate)
			if err == sql.ErrNoRows {
				log.Printf("missing invoice: %s", invoiceNumber)
				fmt.Println("missing invoice: ", invoiceNumber)
				continue
			}
			if err != nil {
				_ = tx.Rollback()
				exitWith("error querying invoice: " + err.Error())
			}
			invDateP, errP := time.Parse(time.RFC3339, invDate)
			if errP != nil {
				_ = tx.Rollback()
				exitWith("parsing error: " + errP.Error())
			}

			// Format ulang agar MySQL menerima
			formattedDate := invDateP.Format("2006-01-02 15:04:05")
			invData = struct {
				ID       int64
				Salesman int64
				TypeInv  int64
				InvDate  string
			}{ID: siID.Int64, Salesman: salesmanID.Int64, TypeInv: typeInv.Int64, InvDate: formattedDate}
			invoiceCache[invoiceNumber] = invData
		}

		// skb
		var issuerWarehouseId int64
		skbID, ok := skbCache[invoiceNumber]
		if !ok {
			err := tx.QueryRow("SELECT skb_id, issuer_warehouse_id FROM list_skb WHERE skb_number = ? LIMIT 1", invoiceNumber).Scan(&skbID, &issuerWarehouseId)
			if err == sql.ErrNoRows {
				log.Printf("missing skb: %s", invoiceNumber)
				fmt.Println("missing skb: ", invoiceNumber)
				continue
			}
			if err != nil {
				_ = tx.Rollback()
				exitWith("error querying skb: " + err.Error())
			}
			skbCache[invoiceNumber] = skbID
		}

		// product
		productCode := getCol(1)
		if productCode == "" {
			fmt.Println("product code kosong")
			continue
		}

		var isConsignImport int64
		var productID int64

		cached, ok := productCache[productCode]
		if ok {
			// ambil data dari cache
			productID = cached.ID
			isConsignImport = cached.IsConsignImport

		} else {
			if productCode == "013632" {
				productCode = "015717"
			}
			if productCode == "011615" {
				productCode = "015588"
			}
			if productCode == "013634" {
				productCode = "015719"
			}
			if productCode == "013631" {
				productCode = "015716"
			}
			if productCode == "014295" {
				productCode = "001060"
			}
			// belum ada di cache → query ke DB
			err := tx.QueryRow(
				"SELECT product_id, is_consign_import FROM list_product WHERE product_code = ? LIMIT 1",
				productCode,
			).Scan(&productID, &isConsignImport)

			if err == sql.ErrNoRows {
				// Insert baru
				res, err2 := tx.Exec(
					"INSERT INTO list_product (product_code, product_name, product_status_id, classification_id, division_id, length_unit, width_unit, height_unit, weight_unit, volume_unit, biggest_conv, smallest_conv, lock_discount, lock_sale, is_need_expired, required_serial_number, product_het, default_hna, product_class, createdAt, createdBy) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), ?)",
					productCode, productCode, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 1, 0, 0, 0, "Reguler", *adminID,
				)
				if err2 != nil {
					_ = tx.Rollback()
					exitWith("error inserting product " + productCode + ":" + err2.Error())
				}

				last, _ := res.LastInsertId()
				productID = last
				isConsignImport = 0

			} else if err != nil {
				_ = tx.Rollback()
				exitWith("error querying product: " + err.Error())
			}

			// simpan ke cache
			productCache[productCode] = ProductCacheData{
				ID:              productID,
				IsConsignImport: isConsignImport,
			}
		}

		parseFloat := func(s string) float64 {
			if s == "" {
				return 0
			}
			f, _ := strconv.ParseFloat(strings.ReplaceAll(s, ",", ""), 64)
			return f
		}

		qty := parseFloat(getCol(3))
		qtyExtra := parseFloat(getCol(4))
		price := parseFloat(getCol(5))
		discR := parseFloat(getCol(6))
		discP := parseFloat(getCol(7))
		batch := getCol(9)
		expDate := getCol(10)

		discRVal := discR / 100 * price
		discPVal := discP / 100 * price
		discVal := discRVal + discPVal
		dpp := price - discVal

		// order
		var count int
		err := tx.QueryRow("SELECT COUNT(1) FROM rel_sales_order_item WHERE sales_order_id = ? AND product_id = ? AND temp_iteration = 1", orderID, productID).Scan(&count)
		if err != nil {
			_ = tx.Rollback()
			exitWith("cek existing order item failed: " + err.Error())
		}

		if count > 0 {
			// Sudah ada, skip insert
			fmt.Println("order sudah pernah insert")
			continue
		}

		var qtyOrder, qtyExtraOrder int64
		errOrd := tx.QueryRow("SELECT qty, qty_extra FROM rel_sales_order_item WHERE sales_order_id = ? AND product_id = ? AND qty != 0", orderID, productID).Scan(&qtyOrder, &qtyExtraOrder)
		if errOrd == sql.ErrNoRows {
			resOrder, err := stmtOrder.Exec(orderID, productID, price, discVal, discRVal, discPVal, discR, discP, dpp, int64(qty))
			if err != nil {
				_ = tx.Rollback()
				exitWith("insert order item failed: " + err.Error())
			}
			groupIDOrder, err := resOrder.LastInsertId()
			if err != nil {
				_ = tx.Rollback()
				exitWith("failed to get last insert id: " + err.Error())
			}
			if qtyExtra > 0 {
				if _, err := stmtOrderExtra.Exec(orderID, productID, price, discVal, discRVal, discPVal, discR, discP, int64(qtyExtra), groupIDOrder); err != nil {
					_ = tx.Rollback()
					exitWith("insert order extra failed: " + err.Error())
				}
			}
		} else {
			newQty := qty + float64(qtyOrder)
			_, errIns := tx.Exec("UPDATE rel_sales_order_item SET qty = ? WHERE sales_order_id = ? AND product_id = ? AND qty_extra = 0", newQty, orderID, productID)
			if errIns != nil {
				_ = tx.Rollback()
				exitWith("update order item failed: " + err.Error())
			}
			if qtyExtra > 0 {
				var rel_id int64
				var extra int64
				errInv := tx.QueryRow("SELECT rel_id, qty_extra FROM rel_sales_order_item WHERE sales_order_id = ? AND product_id = ? AND qty = 0", invData.ID, productID).Scan(&rel_id, &extra)
				if errInv == nil {
					newQty := qtyExtra + float64(extra)
					_, errIns := tx.Exec("UPDATE rel_sales_order_item SET qty_extra = ? WHERE rel_id = ?", newQty, rel_id)
					if errIns != nil {
						_ = tx.Rollback()
						exitWith("update order item failed: " + errIns.Error())
					}
				} else {
					var grpId int64
					errInv := tx.QueryRow("SELECT rel_id FROM rel_sales_order_item WHERE sales_order_id = ? AND product_id = ? AND qty_extra = 0", invData.ID, productID).Scan(&grpId)
					if errInv == nil {
						if _, err := stmtOrderExtra.Exec(orderID, productID, price, discVal, discRVal, discPVal, discR, discP, int64(qtyExtra), grpId); err != nil {
							_ = tx.Rollback()
							exitWith("insert order extra failed: " + err.Error())
						}
					}
				}
			}
		}

		// invoice
		var countInv int
		errInv := tx.QueryRow("SELECT COUNT(1) FROM rel_sales_invoice_item WHERE sales_invoice_id = ? AND product_id = ? AND temp_iteration = 1", invData.ID, productID).Scan(&countInv)
		if errInv != nil {
			_ = tx.Rollback()
			exitWith("cek existing invoice item failed: " + err.Error())
		}

		if countInv > 0 {
			// Sudah ada, skip insert
			continue
		}

		var qtyInvoice, qtyExtraInvoice int64
		if invData.TypeInv != 2 {
			errInv := tx.QueryRow("SELECT qty, qty_extra FROM rel_sales_invoice_item WHERE sales_invoice_id = ? AND product_id = ? AND qty != 0", invData.ID, productID).Scan(&qtyInvoice, &qtyExtraInvoice)
			if errInv == sql.ErrNoRows {
				res, err := stmtInvoice.Exec(
					invData.ID, productID, invData.Salesman, price, nil,
					discVal, discRVal, discPVal, discR, discP, dpp, int64(qty), skbID,
				)
				if err != nil {
					_ = tx.Rollback()
					exitWith("insert invoice item failed: " + err.Error())
				}

				// Ambil last inserted ID (group_id)
				groupID, err := res.LastInsertId()
				if err != nil {
					_ = tx.Rollback()
					exitWith("failed to get last insert id: " + err.Error())
				}
				if qtyExtra > 0 {
					if _, err := stmtInvoiceExtra.Exec(invData.ID, productID, invData.Salesman, price, nil, discVal, discRVal, discPVal, discR, discP, int64(qtyExtra), groupID, skbID); err != nil {
						_ = tx.Rollback()
						exitWith("insert invoice extra failed: " + err.Error())
					}
				}
			} else {
				newQty := qty + float64(qtyInvoice)
				_, errIns := tx.Exec("UPDATE rel_sales_invoice_item SET qty = ? WHERE sales_invoice_id = ? AND product_id = ? AND qty_extra = 0", newQty, invData.ID, productID)
				if errIns != nil {
					_ = tx.Rollback()
					exitWith("update invoice item failed: " + err.Error())
				}
				if qtyExtra > 0 {
					var rel_id int64
					var extra int64
					errInv := tx.QueryRow("SELECT rel_id, qty_extra FROM rel_sales_invoice_item WHERE sales_invoice_id = ? AND product_id = ? AND qty = 0", invData.ID, productID).Scan(&rel_id, &extra)
					if errInv == nil {
						newQty := qtyExtra + float64(extra)
						_, errIns := tx.Exec("UPDATE rel_sales_invoice_item SET qty_extra = ? WHERE rel_id = ?", newQty, rel_id)
						if errIns != nil {
							_ = tx.Rollback()
							exitWith("update invoice item failed: " + errIns.Error())
						}
					} else {
						var grpId int64
						errInv := tx.QueryRow("SELECT rel_id FROM rel_sales_invoice_item WHERE sales_invoice_id = ? AND product_id = ? AND qty_extra = 0", invData.ID, productID).Scan(&grpId)
						if errInv == nil {
							if _, err := stmtInvoiceExtra.Exec(invData.ID, productID, invData.Salesman, price, nil, discVal, discRVal, discPVal, discR, discP, int64(qtyExtra), grpId, skbID); err != nil {
								_ = tx.Rollback()
								exitWith("insert invoice extra failed: " + err.Error())
							}
						}
					}
				}
			}
		} else {
			errInv := tx.QueryRow("SELECT qty, qty_extra FROM rel_sales_invoice_item WHERE sales_invoice_id = ? AND product_id = ? AND batch_number = ? AND qty != 0", invData.ID, productID, batch).Scan(&qtyInvoice, &qtyExtraInvoice)
			if errInv == sql.ErrNoRows {
				res, err := stmtInvoice.Exec(
					invData.ID, productID, invData.Salesman, price, batch,
					discVal, discRVal, discPVal, discR, discP, dpp, int64(qty), skbID,
				)
				if err != nil {
					_ = tx.Rollback()
					exitWith("insert invoice item failed: " + err.Error())
				}

				// Ambil last inserted ID (group_id)
				groupID, err := res.LastInsertId()
				if err != nil {
					_ = tx.Rollback()
					exitWith("failed to get last insert id: " + err.Error())
				}
				if qtyExtra > 0 {
					if _, err := stmtInvoiceExtra.Exec(invData.ID, productID, invData.Salesman, price, batch, discVal, discRVal, discPVal, discR, discP, int64(qtyExtra), groupID, skbID); err != nil {
						_ = tx.Rollback()
						exitWith("insert invoice extra failed: " + err.Error())
					}
				}
			} else {
				newQty := qty + float64(qtyInvoice)
				_, errIns := tx.Exec("UPDATE rel_sales_invoice_item SET qty = ? WHERE sales_invoice_id = ? AND product_id = ? AND qty_extra = 0", newQty, invData.ID, productID)
				if errIns != nil {
					_ = tx.Rollback()
					exitWith("update invoice item failed: " + err.Error())
				}
				if qtyExtra > 0 {
					var rel_id int64
					var extra int64
					errInv := tx.QueryRow("SELECT rel_id, qty_extra FROM rel_sales_invoice_item WHERE sales_invoice_id = ? AND product_id = ? AND qty = 0", invData.ID, productID).Scan(&rel_id, &extra)
					if errInv == nil {
						newQty := qtyExtra + float64(extra)
						_, errIns := tx.Exec("UPDATE rel_sales_invoice_item SET qty_extra = ? WHERE rel_id = ?", newQty, rel_id)
						if errIns != nil {
							_ = tx.Rollback()
							exitWith("update invoice item failed: " + err.Error())
						}
					} else {
						var grpId int64
						errInv := tx.QueryRow("SELECT rel_id FROM rel_sales_invoice_item WHERE sales_invoice_id = ? AND product_id = ? AND qty_extra = 0", invData.ID, productID).Scan(&grpId)
						if errInv == nil {
							if _, err := stmtInvoiceExtra.Exec(invData.ID, productID, invData.Salesman, price, batch, discVal, discRVal, discPVal, discR, discP, int64(qtyExtra), grpId, skbID); err != nil {
								_ = tx.Rollback()
								exitWith("insert invoice extra failed: " + err.Error())
							}
						}
					}
				}
			}
		}

		// skb
		// var countSkb int
		// errSkb := tx.QueryRow("SELECT COUNT(1) FROM rel_skb_item WHERE skb_id = ? AND product_id = ? AND expired_date = ?", skbID, productID).Scan(&countSkb)
		// if errSkb != nil {
		// 	_ = tx.Rollback()
		// 	exitWith("cek existing invoice item failed: " + err.Error())
		// }

		// if countSkb > 0 {
		// 	// Sudah ada, skip insert
		// 	continue
		// }
		if _, err := stmtSkb.Exec(skbID, productID, int64(qty), price, batch, expDate, 5, orderID); err != nil {
			_ = tx.Rollback()
			exitWith("insert skb item failed: " + err.Error())
		}
		if qtyExtra > 0 {
			if _, err := stmtSkbExtra.Exec(skbID, productID, int64(qtyExtra), price, batch, expDate, 5, orderID); err != nil {
				_ = tx.Rollback()
				exitWith("insert skb extra failed: " + err.Error())
			}
		}

		var batchId int64
		err = tx.QueryRow("SELECT batch_id FROM list_product_batch WHERE product_id = ? AND batch_number = ? AND expired_date = ? LIMIT 1",
			productID, batch, expDate).Scan(&batchId)
		if err == sql.ErrNoRows {
			// insert single batch (we need id immediately)
			res, errIns := tx.Exec("INSERT INTO list_product_batch (product_id, batch_number, expired_date, createdAt, createdBy) VALUES (?, ?, ?, NOW(), ?)",
				productID, batch, expDate, 1)
			if errIns != nil {
				_ = tx.Rollback()
				exitWith("insert product batch failed: " + err.Error())
			}
			batchId, _ = res.LastInsertId()
		} else if err != nil {
			_ = tx.Rollback()
			exitWith("insert product batch failed: " + err.Error())
		}

		if isConsignImport == 1 {
			txQty := int64(qty) + int64(qtyExtra)
			txSql := "INSERT INTO list_tx (tx_date, tx_type_id, product_id, warehouse_id, is_consignment, unit, debit, credit, batch_id, skb_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			_, err := tx.Exec(txSql, invData.InvDate, 1, productID, issuerWarehouseId, 1, 1, txQty, 0, batchId, skbID)
			if err != nil {
				_ = tx.Rollback()
				exitWith("insert tx failed: " + err.Error())
			}

			txSqlCred := "INSERT INTO list_tx (tx_date, tx_type_id, product_id, warehouse_id, is_consignment, unit, debit, credit, batch_id, skb_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			_, errCred := tx.Exec(txSqlCred, invData.InvDate, 2, productID, issuerWarehouseId, 1, 1, 0, txQty, batchId, skbID)
			if errCred != nil {
				_ = tx.Rollback()
				exitWith("insert tx failed: " + errCred.Error())
			}

		}

		linkKey := fmt.Sprintf("%d_%d", invData.ID, skbID)
		if !invoiceSKBLinked[linkKey] {
			if _, err := tx.Exec("INSERT IGNORE INTO rel_sales_invoice_skb (sales_invoice_id, skb_id) VALUES (?, ?)", invData.ID, skbID); err != nil {
				_ = tx.Rollback()
				exitWith("insert rel_sales_invoice_skb failed: " + err.Error())
			}
			invoiceSKBLinked[linkKey] = true
		}

		insertedCount++
		if insertedCount%*batchSize == 0 {
			log.Printf("processed %d rows...", insertedCount)
		}
	}

	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		exitWith("commit failed: " + err.Error())
	}

	resp.Success = true
	resp.Message = "Import Sales Invoice Product Success"
	resp.MessageDetail = fmt.Sprintf("Execution Time: %.4f seconds", time.Since(start).Seconds())

	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("Import done: %d rows, %.4fs", insertedCount, time.Since(start).Seconds())
}
