package databath

import (
	"database/sql"
)

type CustomQuery struct {
	Query     string
	InFields  []*Field
	OutFields map[string]*Field
	Type      string
}

func (cq *CustomQuery) Run(db *sql.DB, inFields []interface{}) ([]map[string]interface{}, error) {
	allRows := make([]map[string]interface{}, 0, 0)
	if len(inFields) != len(cq.InFields) {
		return allRows, UserErrorF("Could not run query, got %d parameters, expected %d", len(inFields), len(cq.InFields))
	}
	dbFields := make([]string, len(inFields), len(inFields))
	for i, field := range cq.InFields {
		dbStr, err := field.ToDb(inFields[i], nil)
		if err != nil {
			return allRows, UserErrorF("Could not run query: %s", err.Error())
		}
		dbFields[i] = dbStr
	}
	currentIndex := 0
	replacer := func(q string) string {
		r := dbFields[currentIndex]
		currentIndex += 1
		return r
	}
	sqlString := re_questionmark.ReplaceAllStringFunc(cq.Query, replacer)

	if cq.Type == "exec" {
		res, err := db.Exec(sqlString)
		if err != nil {
			return allRows, err
		}

		insId, err := res.LastInsertId()
		if err != nil {
			insId = 0
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			rowsAffected = 0
		}

		returnRow := map[string]interface{}{
			"insertId":     insId,
			"rowsAffected": rowsAffected,
		}
		allRows = append(allRows, returnRow)
		return allRows, nil
	} else {

		res, err := db.Query(sqlString)
		if err != nil {
			return allRows, err
		}
		defer res.Close()

		sqlColumns, _ := res.Columns()

		for res.Next() {
			thisRow := make(map[string]interface{})
			cols := make([]interface{}, len(sqlColumns), len(sqlColumns))
			for colName, field := range cq.OutFields {
				r := field.GetScanReciever()
				// r is a pointer to a pointer of the correct type (**string, **float64 etc). (NOT a *interface{}, or **interface{} which are different things)
				thisRow[colName] = r
				//cols = append(cols, r)
			}

			for i, colName := range sqlColumns {
				col, ok := thisRow[colName]
				if !ok {
					return allRows, ParseErrF("Custom query column mismatch: %d %s", i, colName)
				}
				cols[i] = col
			}

			// Scan the values - copies the row result into the value pointed by the 'rawValue'
			err := res.Scan(cols...)
			if err != nil {
				return allRows, err
			}

			allRows = append(allRows, thisRow)
		}
		return allRows, nil
	}
}
