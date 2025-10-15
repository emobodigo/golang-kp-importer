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

func RunImportDMFCmd(args []string) {
	fs := flag.NewFlagSet("dmf", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/dmf.xlsx", "path to xlsx file")
	dsn := fs.String("dsn", "", "mysql DSN, e.g. user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true")
	adminID := fs.Int("admin-id", 1, "createdBy admin id")
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

	rows, err := f.GetRows("Sheet1")
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

	type invoiceObj struct {
		outletID        int64
		salesInvoiceID  int64
		trackStatus     int64
		invoicePosition int64
	}

	type dmfItem struct {
		dmfType       int64
		branchID      int64
		loperID       *int64
		courierID     *int64
		receiptNumber string
		dmfNote       string
		invoiceList   []invoiceObj
	}

	groupDmfList := make(map[string]*dmfItem)
	insertedCount := 0

	for r := 1; r < len(rows); r++ {
		cols := rows[r]

		if len(cols) < 17 {
			fmt.Println("column kurang dari 17")
			continue
		}

		dmfDateStr := checkIsTrueEmpty(cols[0])
		if dmfDateStr == nil {
			break
		}

		dmfDate := parseDateForSQL(dmfDateStr)

		dmfTypeStr := checkIsTrueEmpty(cols[1])
		var dmfType int64
		if dmfTypeStr != nil {
			switch strings.ToLower(*dmfTypeStr) {
			case "pengiriman barang":
				dmfType = 1
			case "penerimaan faktur kembali":
				dmfType = 2
			case "penyerahan faktur ke piutang":
				dmfType = 3
			case "penerimaan faktur oleh piutang":
				dmfType = 4
			}
		}

		dmfNote := checkIsTrueEmpty(cols[2])
		branchCode := checkIsTrueEmpty(cols[3])

		if branchCode == nil {
			fmt.Println("branch code kosong")
			continue
		}

		// --- lookup branch ---
		var branchID int64
		err = tx.QueryRow("SELECT branch_id FROM list_branch WHERE branch_code = ? LIMIT 1", *branchCode).Scan(&branchID)
		if err == sql.ErrNoRows {
			fmt.Println("cabang tidak ditemukan: ", branchCode)
			continue
		} else if err != nil {
			_ = tx.Rollback()
			resp.Message = "error querying branch: " + err.Error()
			goto FINISH
		}

		// --- handle loper or courier ---
		var loperID *int64
		var courierID *int64

		loperName := checkIsTrueEmpty(cols[4])
		if loperName != nil && *loperName != "" {
			var existingLoperID int64
			err = tx.QueryRow("SELECT admin_id FROM gemstone_admin WHERE admin_name = ? LIMIT 1", *loperName).Scan(&existingLoperID)
			if err == sql.ErrNoRows {
				res, errIns := tx.Exec(`
					INSERT INTO gemstone_admin 
					(admin_name, admin_fullname, admin_tier_id, password, admin_status) 
					VALUES (?, ?, 30, ?, 1)`,
					*loperName, *loperName, hashPassword())
				if errIns != nil {
					fmt.Println("gagal insert admin")
					continue
				}
				lid, _ := res.LastInsertId()
				loperID = &lid
			} else if err == nil {
				loperID = &existingLoperID
			}
		} else {
			courierName := checkIsTrueEmpty(cols[5])
			if courierName != nil && *courierName != "" {
				var existingCourierID int64
				err = tx.QueryRow("SELECT courier_id FROM list_courier WHERE courier_name = ? LIMIT 1", *courierName).Scan(&existingCourierID)
				if err == sql.ErrNoRows {
					res, errIns := tx.Exec(`
						INSERT INTO list_courier 
						(courier_name, branch_id, is_active, createdBy, createdAt) 
						VALUES (?, ?, 1, ?, NOW())`,
						*courierName, branchID, *adminID)
					if errIns != nil {
						fmt.Println("gagal insert kurir")
						continue
					}
					cid, _ := res.LastInsertId()
					courierID = &cid
				} else if err == nil {
					courierID = &existingCourierID
				}
			}
		}

		// --- lookup invoice ---
		invoiceNumber := checkIsTrueEmpty(cols[7])
		if invoiceNumber == nil {
			fmt.Println("nomor invoice kososng")
			continue
		}

		var salesInvoiceID int64
		err = tx.QueryRow("SELECT sales_invoice_id FROM list_sales_invoice WHERE invoice_number = ? LIMIT 1", *invoiceNumber).Scan(&salesInvoiceID)
		if err == sql.ErrNoRows {
			log.Printf("Missing Invoice: %s\n", *invoiceNumber)
			continue
		} else if err != nil {
			_ = tx.Rollback()
			resp.Message = "error querying invoice: " + err.Error()
			goto FINISH
		}

		// --- lookup outlet ---
		outletCode := checkIsTrueEmpty(cols[8])
		if outletCode == nil {
			fmt.Println("outlet code kososng")
			continue
		}

		var outletID int64
		err = tx.QueryRow("SELECT outlet_id FROM list_outlet WHERE outlet_code = ? LIMIT 1", *outletCode).Scan(&outletID)
		if err == sql.ErrNoRows {
			fmt.Println("outlet tidak ditemukan: ", outletCode)
			continue
		} else if err != nil {
			_ = tx.Rollback()
			resp.Message = "error querying outlet: " + err.Error()
			goto FINISH
		}

		// --- parse track status ---
		trackStatusStr := checkIsTrueEmpty(cols[9])
		var trackStatus int64 = 1
		if trackStatusStr != nil {
			switch strings.ToLower(*trackStatusStr) {
			case "dijadwalkan":
				trackStatus = 1
			case "dalam perjalanan":
				trackStatus = 2
			case "diterima":
				trackStatus = 3
			case "dibatalkan transaksinya":
				trackStatus = 4
			case "penjadwalan ulang":
				trackStatus = 5
			}
		}

		// --- parse invoice position ---
		invoicePositionStr := checkIsTrueEmpty(cols[10])
		var invoicePosition int64 = 4
		if invoicePositionStr != nil {
			switch strings.ToLower(*invoicePositionStr) {
			case "gudang":
				invoicePosition = 1
			case "loper":
				invoicePosition = 2
			case "piutang":
				invoicePosition = 3
			default:
				invoicePosition = 4
			}
		}

		// --- handle dmf admin ---
		dmfAdminName := checkIsTrueEmpty(cols[11])
		var dmfAdminID int64
		if dmfAdminName != nil {
			var existingAdminID int64
			err = tx.QueryRow("SELECT admin_id FROM gemstone_admin WHERE admin_name = ? LIMIT 1", *dmfAdminName).Scan(&existingAdminID)
			if err == sql.ErrNoRows {
				res, errIns := tx.Exec(`
					INSERT INTO gemstone_admin 
					(admin_name, admin_fullname, admin_tier_id, password, admin_status) 
					VALUES (?, ?, 30, ?, 1)`,
					*dmfAdminName, *dmfAdminName, hashPassword())
				if errIns != nil {
					fmt.Println("gagal insert admin: ", dmfAdminName)
					continue
				}
				aid, _ := res.LastInsertId()
				dmfAdminID = aid
			} else if err == nil {
				dmfAdminID = existingAdminID
			}
		}

		// --- group by unique key ---
		uniqueKey := fmt.Sprintf("%s_%d_%d_%d", dmfDate, dmfType, branchID, dmfAdminID)

		if _, ok := groupDmfList[uniqueKey]; !ok {
			groupDmfList[uniqueKey] = &dmfItem{
				dmfType:       dmfType,
				branchID:      branchID,
				loperID:       loperID,
				courierID:     courierID,
				receiptNumber: derefString(checkIsTrueEmpty(cols[6])),
				dmfNote:       derefString(dmfNote),
				invoiceList:   []invoiceObj{},
			}
		}

		invoiceObj := invoiceObj{
			outletID:        outletID,
			salesInvoiceID:  salesInvoiceID,
			trackStatus:     trackStatus,
			invoicePosition: invoicePosition,
		}
		groupDmfList[uniqueKey].invoiceList = append(groupDmfList[uniqueKey].invoiceList, invoiceObj)
	}

	// --- insert grouped data ---
	if len(groupDmfList) > 0 {
		for _, item := range groupDmfList {
			trackNumber := generateTrackInvoiceNumber(tx, item.branchID)

			res, err := tx.Exec(`
				INSERT INTO list_sales_invoice_track_history 
				(track_number, invoice_track_status_id, invoice_track_type_id, branch_id, loper_id, courier_id, receipt_number, markedAt, markedBy, note) 
				VALUES (?, 1, ?, ?, ?, ?, ?, NOW(), ?, ?)`,
				trackNumber, item.dmfType, item.branchID, item.loperID, item.courierID, item.receiptNumber, *adminID, item.dmfNote)

			if err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting track history: " + err.Error()
				goto FINISH
			}

			trackHistoryID, _ := res.LastInsertId()

			for _, invoice := range item.invoiceList {
				_, err := tx.Exec(`
					INSERT INTO rel_track_history_invoice 
					(track_history_id, outlet_id, sales_invoice_id, track_status_id, track_position_id, date_track, admin_track, track_used_id) 
					VALUES (?, ?, ?, ?, ?, ?, ?, NULL)`,
					trackHistoryID, invoice.outletID, invoice.salesInvoiceID, invoice.trackStatus, invoice.invoicePosition, time.Now().Format("2006-01-02"), *adminID)

				if err != nil {
					_ = tx.Rollback()
					resp.Message = "error inserting invoice track history: " + err.Error()
					goto FINISH
				}
				insertedCount++
			}
		}
	}

	// commit
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		resp.Message = "db commit error: " + err.Error()
		goto FINISH
	}

	resp.Success = true
	resp.Message = "Import DMF Success"
	resp.MessageDetail = fmt.Sprintf("Total %d rows inserted. Execution Time: %.4fs", insertedCount, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import dmf complete: %d invoice rows, time=%.4fs\n", insertedCount, time.Since(start).Seconds())
}

// Helper functions
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func hashPassword() string {
	// Implementation using golang.org/x/crypto/bcrypt
	// For now, return placeholder
	return "$2y$10$BpYtQGwQSSTM79aUVJdW7.gwdOCJ.cY29g.sc1KS3qusyU8U4eHFu"
}

func generateTrackInvoiceNumber(tx *sql.Tx, branchID int64) string {
	// Generate unique track number - implement according to your logic
	return fmt.Sprintf("DMF-%d-%d", branchID, time.Now().UnixNano())
}
