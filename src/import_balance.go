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

func RunImportBeginningBalanceCmd(args []string) {
	fs := flag.NewFlagSet("balance", flag.ExitOnError)
	filePath := fs.String("file", "./uploads/balance.xlsx", "path to xlsx file")
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
	branchCache := make(map[string]int64)
	principalCache := make(map[string]int64)
	accountTypeCache := make(map[string]int64)
	bankAccountCache := make(map[string]int64)

	// Batch containers
	cols := []string{
		"ledger_date", "ledger_number", "branch_id", "principal_id",
		"account_type_id", "bank_account_id", "debit", "note",
		"createdAt", "createdBy", "is_verified", "group_id", "snapshot_start_balance",
	}
	batchRows := [][]interface{}{}
	insertedCount := 0
	rowIndex := 0
	groupIndex := 0

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
		// indices: 0 ledger_date, 1 branch_code, 2 principal_code, 3 account_type_name,
		//          4 bank_account_number, 5 balance, 6 ledger_note
		if len(rowData) < 6 {
			fmt.Println("colum lebih kecil dari 6")
			continue
		}

		ledgerDatePtr := getCol(0)
		if ledgerDatePtr == nil {
			fmt.Println("tidak ada tanggal ledger")
			continue // stop if no date
		}

		branchCodePtr := getCol(1)
		principalCodePtr := getCol(2)
		accountTypeNamePtr := getCol(3)
		bankAccountNumberPtr := getCol(4)
		balancePtr := getCol(5)
		ledgerNotePtr := getCol(6)

		if branchCodePtr == nil || accountTypeNamePtr == nil {
			fmt.Println(branchCodePtr, principalCodePtr, accountTypeNamePtr, bankAccountNumberPtr)
			fmt.Println("input kosong")
			continue
		}

		groupIndex++

		// Parse ledger date
		ledgerDate := parseDateForSQL(ledgerDatePtr)
		if ledgerDate == "" {
			ledgerDate = time.Now().Format("2006-01-02")
		}

		// Generate ledger number (simplified - should use proper generator)
		ledgerNumber := fmt.Sprintf("LEDGER-%d-%d", time.Now().Unix(), groupIndex)

		branchCode := strings.TrimSpace(*branchCodePtr)

		principalCode := ""
		if principalCodePtr != nil {
			principalCode = strings.TrimSpace(*principalCodePtr)
		}
		accountTypeName := strings.TrimSpace(*accountTypeNamePtr)
		bankAccountNumber := ""
		if bankAccountNumberPtr != nil {
			bankAccountNumber = strings.TrimSpace(*bankAccountNumberPtr)
		}

		// Get or cache branch
		var branchID int64
		if cached, ok := branchCache[branchCode]; ok {
			branchID = cached
		} else {
			err = tx.QueryRow(`
				SELECT branch_id 
				FROM list_branch 
				WHERE branch_code = ? 
				LIMIT 1
			`, branchCode).Scan(&branchID)

			if err == sql.ErrNoRows {
				fmt.Printf("Branch not found: %s\n", branchCode)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying branch: " + err.Error()
				goto FINISH
			}
			branchCache[branchCode] = branchID
		}

		// Get or cache principal
		var principalID sql.NullInt64

		if principalCodePtr != nil {

			if cached, ok := principalCache[principalCode]; ok {
				principalID = sql.NullInt64{Int64: cached, Valid: true}
			} else {
				var pid int64
				err = tx.QueryRow(`
			SELECT principal_id 
			FROM list_principal 
			WHERE principal_code = ? 
			LIMIT 1
		`, principalCode).Scan(&pid)

				if err == sql.ErrNoRows {
					fmt.Printf("Principal not found: %s\n", principalCode)
					// tetap lanjut tapi principalID = NULL
					principalID = sql.NullInt64{Valid: false}
				} else if err != nil {
					_ = tx.Rollback()
					resp.Message = "error querying principal: " + err.Error()
					goto FINISH
				} else {
					principalID = sql.NullInt64{Int64: pid, Valid: true}
					principalCache[principalCode] = pid
				}
			}
		} else {
			principalID = sql.NullInt64{Valid: false}
		}

		// Get or cache account type
		if accountTypeName == "Bank Kas Besar" {
			accountTypeName = "Bank Besar"
		}
		if accountTypeName == "Bank Kas Kecil" {
			accountTypeName = "Bank Kecil"
		}
		var accountTypeID int64
		if cached, ok := accountTypeCache[accountTypeName]; ok {
			accountTypeID = cached
		} else {
			err = tx.QueryRow(`
				SELECT account_type_id 
				FROM list_account_type 
				WHERE account_type_name = ? 
				LIMIT 1
			`, accountTypeName).Scan(&accountTypeID)

			if err == sql.ErrNoRows {
				fmt.Printf("Account type not found: %s\n", accountTypeName)
				continue
			} else if err != nil {
				_ = tx.Rollback()
				resp.Message = "error querying account type: " + err.Error()
				goto FINISH
			}
			accountTypeCache[accountTypeName] = accountTypeID
		}

		// Get or cache bank account
		var bankAccountID sql.NullInt64
		if bankAccountNumberPtr != nil {
			if cached, ok := bankAccountCache[bankAccountNumber]; ok {
				bankAccountID = sql.NullInt64{Int64: cached, Valid: true}
			} else {
				var bId int64
				err = tx.QueryRow(`
				SELECT bank_account_id 
				FROM list_bank_account 
				WHERE account_number = ? 
				LIMIT 1
			`, bankAccountNumber).Scan(&bId)

				if err == sql.ErrNoRows {
					fmt.Printf("Bank account not found: %s\n", bankAccountNumber)
					continue
				} else if err != nil {
					_ = tx.Rollback()
					resp.Message = "error querying bank account: " + err.Error()
					goto FINISH
				} else {
					bankAccountID = sql.NullInt64{Int64: bId, Valid: true}
					bankAccountCache[bankAccountNumber] = bId
				}
			}
		}

		balance := denormFloat(balancePtr)

		ledgerNote := ""
		if ledgerNotePtr != nil {
			ledgerNote = strings.TrimSpace(*ledgerNotePtr)
		}

		createdAt := time.Now().Format("2006-01-02 15:04:05")

		// Prepare row values in same order as cols
		rowVals := []interface{}{
			ledgerDate,    // ledger_date
			ledgerNumber,  // ledger_number
			branchID,      // branch_id
			principalID,   // principal_id
			accountTypeID, // account_type_id
			bankAccountID, // bank_account_id
			balance,       // debit
			ledgerNote,    // note
			createdAt,     // createdAt
			*adminID,      // createdBy
			1,             // is_verified (always 1)
			groupIndex,    // group_id
			balance,       // snapshot_start_balance (same as balance)
		}
		batchRows = append(batchRows, rowVals)

		// Flush batch when size reached
		if len(batchRows) >= *batchSize {
			base := "INSERT INTO `list_cash_ledger`"
			q, sqlArgs := buildMultiInsert(base, cols, batchRows)
			if _, err := tx.Exec(q, sqlArgs...); err != nil {
				_ = tx.Rollback()
				resp.Message = "error inserting batch to list_cash_ledger: " + err.Error()
				goto FINISH
			}
			insertedCount += len(batchRows)
			batchRows = [][]interface{}{}
		}
	}

	// Flush remaining
	if len(batchRows) > 0 {
		base := "INSERT INTO `list_cash_ledger`"
		q, sqlArgs := buildMultiInsert(base, cols, batchRows)
		if _, err := tx.Exec(q, sqlArgs...); err != nil {
			_ = tx.Rollback()
			resp.Message = "error inserting final batch to list_cash_ledger: " + err.Error()
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
	resp.Message = "Import Beginning Balance Success"
	resp.MessageDetail = fmt.Sprintf("Total %d ledger entries inserted. Execution Time: %.4fs", insertedCount, time.Since(start).Seconds())

FINISH:
	out, _ := json.Marshal(resp)
	fmt.Println(string(out))
	log.Printf("import beginning balance complete: %d entries, time=%.4fs\n", insertedCount, time.Since(start).Seconds())
}
