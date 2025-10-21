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
		 dpp, unit, qty, qty_extra, temp_iteration)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 0, 2)
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
		 dpp, unit, qty, qty_extra, group_id, temp_iteration)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, 0, ?, ?, 2)
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
	}{}
	skbCache := map[string]int64{}
	productCache := map[string]int64{}
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
			err := tx.QueryRow("SELECT sales_invoice_id, salesman_id, sales_invoice_type_id FROM list_sales_invoice WHERE sales_invoice_number = ? LIMIT 1", invoiceNumber).
				Scan(&siID, &salesmanID, &typeInv)
			if err == sql.ErrNoRows {
				log.Printf("missing invoice: %s", invoiceNumber)
				fmt.Println("missing invoice: ", invoiceNumber)
				continue
			}
			if err != nil {
				_ = tx.Rollback()
				exitWith("error querying invoice: " + err.Error())
			}
			invData = struct {
				ID       int64
				Salesman int64
				TypeInv  int64
			}{ID: siID.Int64, Salesman: salesmanID.Int64, TypeInv: typeInv.Int64}
			invoiceCache[invoiceNumber] = invData
		}

		// skb
		skbID, ok := skbCache[invoiceNumber]
		if !ok {
			err := tx.QueryRow("SELECT skb_id FROM list_skb WHERE skb_number = ? LIMIT 1", invoiceNumber).Scan(&skbID)
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

		productID, ok := productCache[productCode]
		if !ok {
			err := tx.QueryRow("SELECT product_id FROM list_product WHERE product_code = ? LIMIT 1", productCode).Scan(&productID)
			if err == sql.ErrNoRows {
				res, err2 := tx.Exec("INSERT INTO list_product (product_code, product_name, createdAt, createdBy) VALUES (?, ?, NOW(), ?)",
					productCode, productCode, *adminID)
				if err2 != nil {
					_ = tx.Rollback()
					exitWith("error inserting product: " + err2.Error())
				}
				last, _ := res.LastInsertId()
				productID = last
			} else if err != nil {
				_ = tx.Rollback()
				exitWith("error querying product: " + err.Error())
			}
			productCache[productCode] = productID
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
					_, errIns := tx.Exec("UPDATE rel_sales_order_item SET qty_extra = ? WHERE rel_id", newQty, rel_id)
					if errIns != nil {
						_ = tx.Rollback()
						exitWith("update order item failed: " + err.Error())
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
					discVal, discRVal, discPVal, discR, discP, dpp, int64(qty),
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
					if _, err := stmtInvoiceExtra.Exec(invData.ID, productID, invData.Salesman, price, nil, discVal, discRVal, discPVal, discR, discP, int64(qtyExtra), groupID); err != nil {
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
						_, errIns := tx.Exec("UPDATE rel_sales_invoice_item SET qty_extra = ? WHERE rel_id", newQty, rel_id)
						if errIns != nil {
							_ = tx.Rollback()
							exitWith("update invoice item failed: " + err.Error())
						}
					} else {
						var grpId int64
						errInv := tx.QueryRow("SELECT rel_id FROM rel_sales_invoice_item WHERE sales_invoice_id = ? AND product_id = ? AND qty_extra = 0", invData.ID, productID).Scan(&grpId)
						if errInv == nil {
							if _, err := stmtInvoiceExtra.Exec(invData.ID, productID, invData.Salesman, price, nil, discVal, discRVal, discPVal, discR, discP, int64(qtyExtra), grpId); err != nil {
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
					discVal, discRVal, discPVal, discR, discP, dpp, int64(qty),
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
					if _, err := stmtInvoiceExtra.Exec(invData.ID, productID, invData.Salesman, price, batch, discVal, discRVal, discPVal, discR, discP, int64(qtyExtra), groupID); err != nil {
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
						_, errIns := tx.Exec("UPDATE rel_sales_invoice_item SET qty_extra = ? WHERE rel_id", newQty, rel_id)
						if errIns != nil {
							_ = tx.Rollback()
							exitWith("update invoice item failed: " + err.Error())
						}
					} else {
						var grpId int64
						errInv := tx.QueryRow("SELECT rel_id FROM rel_sales_invoice_item WHERE sales_invoice_id = ? AND product_id = ? AND qty_extra = 0", invData.ID, productID).Scan(&grpId)
						if errInv == nil {
							if _, err := stmtInvoiceExtra.Exec(invData.ID, productID, invData.Salesman, price, batch, discVal, discRVal, discPVal, discR, discP, int64(qtyExtra), grpId); err != nil {
								_ = tx.Rollback()
								exitWith("insert invoice extra failed: " + err.Error())
							}
						}
					}
				}
			}
		}

		// skb
		var countSkb int
		errSkb := tx.QueryRow("SELECT COUNT(1) FROM rel_skb_item WHERE skb_id = ?", skbID).Scan(&countSkb)
		if errSkb != nil {
			_ = tx.Rollback()
			exitWith("cek existing invoice item failed: " + err.Error())
		}

		if countSkb > 0 {
			// Sudah ada, skip insert
			continue
		}
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
