package src

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

func checkIsTrueEmpty(v string) *string {
	// replicate PHP check_is_true_empty
	if strings.TrimSpace(v) == "" {
		return nil
	}
	// replace multiple whitespace by single space and trim
	t := strings.Join(strings.Fields(strings.TrimSpace(v)), " ")
	if t == "" {
		return nil
	}
	return &t
}

func denormalizeNumber(v *string) string {
	if v == nil {
		return "0"
	}
	s := *v
	if s == "" {
		return "0"
	}
	// remove commas
	return strings.ReplaceAll(s, ",", "")
}

func checkDuplicate(tx *sql.Tx, table, field string, value string) (bool, error) {
	if value == "" {
		return false, nil
	}
	q := fmt.Sprintf("SELECT 1 FROM `%s` WHERE `%s` = ? LIMIT 1;", table, field)
	row := tx.QueryRow(q, value)
	var dummy int
	err := row.Scan(&dummy)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// checkImportColumn replicates PHP logic for certain tables.
// options is a map of option flags like {"internal": "true", "principal_id": "2"}
func checkImportColumn(tx *sql.Tx, fieldName, tableName, value string, options map[string]string) (sql.NullInt64, error) {
	// normalize value
	if value == "" {
		value = ""
	}

	// search existing row by LIKE
	searchQ := fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` LIKE ? ORDER BY 1 ASC LIMIT 1;", tableName, fieldName)
	likeVal := "%" + value + "%"
	rows, err := tx.Query(searchQ, likeVal)
	if err != nil {
		return sql.NullInt64{}, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	if rows.Next() {
		// get first column value (primary key)
		// dynamic scan: read first col as int64
		colsVals := make([]interface{}, len(cols))
		colsPtr := make([]interface{}, len(cols))
		for i := range colsVals {
			colsPtr[i] = &colsVals[i]
		}
		if err := rows.Scan(colsPtr...); err != nil {
			return sql.NullInt64{}, err
		}
		// take first field as id
		switch v := colsVals[0].(type) {
		case int64:
			return sql.NullInt64{Int64: v, Valid: true}, nil
		case []byte:
			n, _ := strconv.ParseInt(string(v), 10, 64)
			return sql.NullInt64{Int64: n, Valid: true}, nil
		default:
			// fallback: try convert fmt.Sprint
			n, _ := strconv.ParseInt(fmt.Sprint(v), 10, 64)
			return sql.NullInt64{Int64: n, Valid: true}, nil
		}
	}

	// if not found, we need to insert according to table name (mimic PHP cases)
	switch tableName {
	case "list_branch":
		branchName := strings.Title(strings.ToLower(value))
		// find town_id
		townQ := "SELECT town_id FROM list_town WHERE town_name LIKE ? ORDER BY 1 ASC LIMIT 1;"
		townRow := tx.QueryRow(townQ, "%"+branchName+"%")
		townID := int64(1)
		if err := townRow.Scan(&townID); err != nil {
			// keep townID=1 on error or not found
			townID = 1
		}
		insertQ := "INSERT INTO `list_branch` (`" + fieldName + "`, `branch_code`, `branch_postal_code`, `town_id`, `createdAt`, `createdBy`) VALUES (?, '00', '0000', ?, NOW(), '1')"
		res, err := tx.Exec(insertQ, branchName, townID)
		if err != nil {
			return sql.NullInt64{}, err
		}
		id, _ := res.LastInsertId()
		return sql.NullInt64{Int64: id, Valid: true}, nil

	case "list_outlet_segment":
		segmentTypeID := 1
		if _, ok := options["external"]; ok {
			segmentTypeID = 2
		}
		insertQ := "INSERT INTO `list_outlet_segment` (`" + fieldName + "`, `segment_type_id`, `segment_classification_id`, `createdAt`) VALUES (?, ?, 1, NOW())"
		res, err := tx.Exec(insertQ, value, segmentTypeID)
		if err != nil {
			return sql.NullInt64{}, err
		}
		id, _ := res.LastInsertId()
		return sql.NullInt64{Int64: id, Valid: true}, nil

	case "list_town":
		// PHP returns null
		return sql.NullInt64{Valid: false}, nil

	case "list_principal_division":
		//sdivisionParts := strings.Split(value, "-")
		principalID := sql.NullInt64{Valid: false}
		if pid, ok := options["principal_id"]; ok && pid != "" {
			if p, err := strconv.ParseInt(pid, 10, 64); err == nil {
				principalID = sql.NullInt64{Int64: p, Valid: true}
			}
		}
		insertQ := "INSERT INTO `list_principal_division` (`" + fieldName + "`, `old_code`, `division_code`, `principal_id`) VALUES (?, NULL, NULL, ?)"
		var pidVal interface{}
		if principalID.Valid {
			pidVal = principalID.Int64
		} else {
			pidVal = nil
		}
		res, err := tx.Exec(insertQ, value, pidVal)
		if err != nil {
			return sql.NullInt64{}, err
		}
		id, _ := res.LastInsertId()
		return sql.NullInt64{Int64: id, Valid: true}, nil

	case "list_tag":
		insertQ := "INSERT INTO `list_tag` (`" + fieldName + "`, `tag_type_id`, `createdAt`, `createdBy`) VALUES (?, 1, NOW(), 1)"
		res, err := tx.Exec(insertQ, value)
		if err != nil {
			return sql.NullInt64{}, err
		}
		id, _ := res.LastInsertId()
		return sql.NullInt64{Int64: id, Valid: true}, nil

	default:
		// generic insert; check if table has createdAt/createdBy
		colsQ := fmt.Sprintf("SHOW COLUMNS FROM `%s`;", tableName)
		crows, err := tx.Query(colsQ)
		if err != nil {
			return sql.NullInt64{}, err
		}
		defer crows.Close()
		hasCreatedAt := false
		hasCreatedBy := false
		for crows.Next() {
			var field, colType, isNull, key, defaultVal, extra string
			if err := crows.Scan(&field, &colType, &isNull, &key, &defaultVal, &extra); err == nil {
				if field == "createdAt" {
					hasCreatedAt = true
				}
				if field == "createdBy" {
					hasCreatedBy = true
				}
			}
		}
		if hasCreatedAt && hasCreatedBy {
			insertQ := fmt.Sprintf("INSERT INTO `%s` (`%s`, `createdAt`, `createdBy`) VALUES (?, NOW(), 1)", tableName, fieldName)
			res, err := tx.Exec(insertQ, value)
			if err != nil {
				return sql.NullInt64{}, err
			}
			id, _ := res.LastInsertId()
			return sql.NullInt64{Int64: id, Valid: true}, nil
		}
		// fallback simple insert
		insertQ := fmt.Sprintf("INSERT INTO `%s` (`%s`) VALUES (?)", tableName, fieldName)
		res, err := tx.Exec(insertQ, value)
		if err != nil {
			return sql.NullInt64{}, err
		}
		id, _ := res.LastInsertId()
		return sql.NullInt64{Int64: id, Valid: true}, nil
	}
}

func updateActivity(tx *sql.Tx, logID interface{}, label string, link interface{}, metaData interface{}) error {
	// Only update when logID is not false / not nil
	if logID == nil {
		return nil
	}
	// expecting numeric or string id
	q := "UPDATE `gemstone_activity_log` SET `label` = ?, `target_link` = ?, `meta_data` = ?, `legacy_log` = 0 WHERE `log_id` = ?;"
	_, err := tx.Exec(q, label, link, metaData, logID)
	return err
}

func buildMultiInsert(base string, cols []string, rows [][]interface{}) (string, []interface{}) {
	// base e.g. "INSERT INTO `list_outlet`"
	// cols: column names
	// rows: each row is []interface{}
	values := []string{}
	args := []interface{}{}
	for _, r := range rows {
		ph := "(" + strings.TrimRight(strings.Repeat("?,", len(r)), ",") + ")"
		values = append(values, ph)
		for _, v := range r {
			args = append(args, v)
		}
	}
	query := fmt.Sprintf("%s (`%s`) VALUES %s", base, strings.Join(cols, "`, `"), strings.Join(values, ","))
	return query, args
}

func getProductByName(tx *sql.Tx, productName string) (*sql.Row, error) {
	q := "SELECT * FROM list_product WHERE product_name = ? LIMIT 1"
	row := tx.QueryRow(q, productName)
	return row, nil
}

func getSupplierByName(tx *sql.Tx, name string) (*sql.Row, error) {
	q := "SELECT * FROM list_supplier WHERE supplier_name LIKE ? LIMIT 1"
	row := tx.QueryRow(q, "%"+name+"%")
	return row, nil
}

func getProductGroupByName(tx *sql.Tx, name string) (*sql.Row, error) {
	q := "SELECT * FROM list_tag WHERE tag_name LIKE ? LIMIT 1"
	row := tx.QueryRow(q, "%"+name+"%")
	return row, nil
}

func parseDateForSQL(cell *string) interface{} {
	if cell == nil {
		return nil
	}
	s := strings.TrimSpace(*cell)
	if s == "" {
		return nil
	}

	// 1) coba parse sebagai Excel serial (angka)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		if t, err2 := excelize.ExcelDateToTime(f, false); err2 == nil {
			return t.Format("2006-01-02")
		}
	}

	// 2) coba beberapa layout umum
	layouts := []string{
		"2006-01-02",
		"02/01/2006", "2/1/2006",
		"02-01-2006",
		"02 Jan 2006", "2 Jan 2006",
		"2006/01/02",
		"02.01.2006",
		"2-Jan-2006",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Format("2006-01-02")
		}
	}

	// 3) fallback: jika pattern dd/mm/yyyy (mis. "31/12/2025") — parse manual
	if strings.Contains(s, "/") {
		parts := strings.Split(s, "/")
		if len(parts) == 3 {
			d := strings.TrimSpace(parts[0])
			m := strings.TrimSpace(parts[1])
			y := strings.TrimSpace(parts[2])

			// jika tahun 2-digit, expand ke 20xx/19xx (opsional)
			if len(y) == 2 {
				if yi, err := strconv.Atoi(y); err == nil {
					if yi >= 50 {
						y = fmt.Sprintf("19%02d", yi)
					} else {
						y = fmt.Sprintf("20%02d", yi)
					}
				}
			}
			di, err1 := strconv.Atoi(d)
			mi, err2 := strconv.Atoi(m)
			yi, err3 := strconv.Atoi(y)
			if err1 == nil && err2 == nil && err3 == nil {
				t := time.Date(yi, time.Month(mi), di, 0, 0, 0, 0, time.UTC)
				return t.Format("2006-01-02")
			}
		}
	}

	// Tidak dikenali: log peringatan dan masukkan NULL agar tidak error DB
	log.Printf("warning: cannot parse date '%s', will insert NULL\n", s)
	return nil
}

func denormInt(p *string) int64 {
	if p == nil {
		return 0
	}
	s := strings.TrimSpace(*p)
	if s == "" {
		return 0
	}
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, ".", "") // if your numbers use thousands dot
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v
	}
	// try float -> int
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f)
	}
	return 0
}

func parseExcelDate(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// dd/mm/yyyy
	if t, err := time.Parse("02/01/2006", s); err == nil {
		return t.Format("2006-01-02")
	}
	// yyyy-mm-dd
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t.Format("2006-01-02")
	}
	// try excel serial encoded as number in string
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		if t, err2 := excelize.ExcelDateToTime(f, false); err2 == nil {
			return t.Format("2006-01-02")
		}
	}
	// fallback: return original (DB may reject invalid)
	return s
}

func denormFloat(val *string) float64 {
	if val == nil {
		return 0
	}

	s := strings.TrimSpace(*val)
	if s == "" {
		return 0
	}

	// Hapus semua spasi
	s = strings.ReplaceAll(s, " ", "")

	// Normalisasi angka dengan koma (contoh: "1.234,56" -> "1234.56")
	// deteksi format Eropa (koma sebagai decimal)
	if strings.Contains(s, ",") && strings.Contains(s, ".") {
		lastComma := strings.LastIndex(s, ",")
		lastDot := strings.LastIndex(s, ".")
		if lastComma > lastDot {
			s = strings.ReplaceAll(s, ".", "")
			s = strings.ReplaceAll(s, ",", ".")
		} else {
			s = strings.ReplaceAll(s, ",", "")
		}
	} else if strings.Contains(s, ",") {
		// anggap koma adalah desimal
		s = strings.ReplaceAll(s, ",", ".")
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

func containsInt64(slice []int64, val int64) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}
