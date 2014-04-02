package sync

import (
	"database/sql"
	"fmt"
	"github.com/daemonl/go_lib/databath"
	"github.com/daemonl/go_lib/databath/types"
	"log"
	"reflect"
	"strings"
)

func doErr(err error) {
	if err != nil {
		panic(err)
	}
}

type TableStatus struct {
	Name            string  `sql:"Name"`
	Engine          string  `sql:"Engine"`
	Version         *uint64 `sql:"Version"`
	Row_format      *string `sql:"Row_format"`
	Rows            *uint64 `sql:"Rows"`
	Avg_row_length  *uint64 `sql:"Avg_row_length"`
	Data_length     *uint64 `sql:"Data_length"`
	Max_data_length *uint64 `sql:"Max_data_length"`
	Index_length    *uint64 `sql:"Index_length"`
	Data_free       *uint64 `sql:"Data_free"`
	Auto_increment  *uint64 `sql:"Auto_increment"`
	Create_time     *string `sql:"Create_time"`
	Update_time     *string `sql:"Update_time"`
	Check_time      *string `sql:"Check_time"`
	Collation       *string `sql:"Collation"`
	Checksum        *string `sql:"Checksum"`
	Create_options  *string `sql:"Create_options"`
	Comment         *string `sql:"Comment"`
}

type Column struct {
	Field   string  `sql:"Field"`
	Type    string  `sql:"Type"`
	Null    string  `sql:"Null"`
	Key     *string `sql:"Key"`
	Default *string `sql:"Default"`
	Extra   *string `sql:"Extra"`
}

type Index struct {
	ConstraintName       string  `sql:"CONSTRAINT_NAME"`
	TableName            *string `sql:"TABLE_NAME"`
	ConstraintType       *string `sql:"CONSTRAINT_TYPE"`
	ColumnName           *string `sql:"COLUMN_NAME"`
	ReferencedTableName  *string `sql:"REFERENCED_TABLE_NAME"`
	ReferencedColumnName *string `sql:"REFERENCED_COLUMN_NAME"`
	Used                 bool
}

type SyncError struct {
	Message string
}

func (e *SyncError) Error() string {
	return e.Message
}

var execString string = ""

func (c *Column) GetString() string {
	built := c.Type
	if c.Null == "NO" {
		built += " NOT NULL"
	} else {
		built += " NULL"
	}
	if c.Extra != nil {
		built += " " + *c.Extra
	}
	built = strings.TrimSpace(built)
	return strings.ToUpper(built)
}

func ScanToStruct(res *sql.Rows, obj interface{}, tag string) error {
	if len(tag) == 0 {
		tag = "sql"
	}
	rv := reflect.ValueOf(obj)
	rt := reflect.TypeOf(obj)

	if reflect.Indirect(rv).Kind().String() != "struct" {
		panic("KIND NOT STRUCT" + rv.Kind().String())
	}

	valueElm := rv.Elem()

	maxElements := rt.Elem().NumField()
	//scanVals := make([]interface{}, maxElements, maxElements)
	cols, err := res.Columns()
	if err != nil {
		return err
	}

	scanVals := map[string]interface{}{}

	for i := 0; i < maxElements; i++ {
		interf := valueElm.Field(i).Addr().Interface()
		sqlTag := valueElm.Type().Field(i).Tag.Get(tag)
		if len(sqlTag) < 1 || sqlTag == "-" {
			continue
		}
		scanVals[sqlTag] = interf
	}
	orderedScanVals := make([]interface{}, len(cols), len(cols))
	for i, colName := range cols {
		interfPtr, ok := scanVals[colName]
		if !ok {
			return &SyncError{fmt.Sprintf("SQL Column '%s' had no matching struct tag", colName)}
		}
		orderedScanVals[i] = interfPtr
	}
	err = res.Scan(orderedScanVals...)

	if err != nil {
		return err
	}
	return nil
}

func MustExecF(now bool, db *sql.DB, format string, parameters ...interface{}) {
	q := fmt.Sprintf(format, parameters...)
	log.Println("EXEC: " + q)
	if now {
		_, err := db.Exec(q)
		doErr(err)
	} else {
		execString += fmt.Sprintf("%s;\n", q)

	}

	//log.Printf("RES: %d %d;", res.RowsAffected(), res.LastInsertId())
}

func SyncDb(db *sql.DB, model *databath.Model, now bool) {

	// CREATE DATABASE IF NOT EXISTS #{config.db.database}
	// USE #{config.db.database}
	// Probably won't work - the connection is set to a database.

	res, err := db.Query(`SHOW TABLE STATUS WHERE Engine != 'InnoDB'`)
	doErr(err)

	for res.Next() {
		table := TableStatus{}
		err := ScanToStruct(res, &table, "sql")
		doErr(err)
		MustExecF(now, db, "ALTER TABLE %s ENGINE = 'InnoDB'", table.Name)
	}
	res.Close()

	for collectionName, collection := range model.Collections {
		log.Printf("COLLECTION: %s\n", collectionName)
		res, err := db.Query(`SHOW TABLE STATUS WHERE Name = ?`, collectionName)
		doErr(err)
		if res.Next() {
			log.Println("UPDATE TABLE")
			// UPDATE!

			log.Println("Get table info")
			indexRes, err := db.Query(`
SELECT
  c.CONSTRAINT_NAME,
  c.TABLE_NAME,
  c.CONSTRAINT_TYPE,
  k.COLUMN_NAME,
  k.REFERENCED_TABLE_NAME,
  k.REFERENCED_COLUMN_NAME 
FROM information_schema.TABLE_CONSTRAINTS c
LEFT JOIN information_schema.KEY_COLUMN_USAGE k 
  ON c.CONSTRAINT_NAME = k.CONSTRAINT_NAME 
  AND c.TABLE_SCHEMA = k.TABLE_SCHEMA 
  AND c.TABLE_NAME = k.TABLE_NAME 
WHERE c.TABLE_SCHEMA = DATABASE() AND c.TABLE_NAME = "` + collectionName + `";
`)

			doErr(err)
			indexes := []*Index{}
			for indexRes.Next() {
				index := Index{}
				err := ScanToStruct(indexRes, &index, "sql")
				doErr(err)
				indexes = append(indexes, &index)
			}

			log.Println("Scan Cols")

			deferredStatements := []string{}

			for colName, field := range collection.Fields {
				log.Printf("Scan %s.%s\n", collectionName, colName)

				showRes, err := db.Query(`SHOW COLUMNS FROM `+collectionName+` WHERE Field = ?`, colName)
				doErr(err)
				if showRes.Next() {
					col := Column{}
					err := ScanToStruct(showRes, &col, "sql")
					doErr(err)
					colStr := col.GetString()
					modelStr := field.GetMysqlDef()
					if colStr != modelStr {
						log.Printf("'%s' '%s'\n", colStr, modelStr)
						MustExecF(now, db, "ALTER TABLE %s CHANGE COLUMN %s %s %s",
							collectionName, colName, colName, modelStr)
					}
				} else {
					MustExecF(now, db, "ALTER TABLE %s ADD `%s` %s", collectionName, colName, field.GetMysqlDef())
				}
				showRes.Close()

				refField, ok := field.Impl.(*types.FieldRef)
				if ok {
					var matchedIndex *Index = nil
					for _, index := range indexes {
						// If Matches

						if index.ReferencedTableName != nil && colName == *index.ColumnName && *index.ReferencedTableName == refField.Collection {
							matchedIndex = index
							matchedIndex.Used = true
							break
						}

					}

					if matchedIndex == nil {
						// Create It.
						// Is it creatable with the current data?
						badRowsRes, err := db.Query(fmt.Sprintf(`SELECT id, %s FROM %s WHERE %s IS NOT NULL AND %s NOT IN (SELECT %s FROM %s)`, colName, collectionName, colName, colName, "id", refField.Collection))
						if err != nil {
							log.Printf("Error on FK check for %s.%s\n", collectionName, colName)

						} else {

							hasBad := false
							for badRowsRes.Next() {
								hasBad = true
								id := 0
								fkVal := 0
								badRowsRes.Scan(&id, &fkVal)
								log.Printf("Foreign Key Test Fail: Entry %d for %s.%s references %s.id = %d, which doesn't exist\n", id, collectionName, colName, refField.Collection, fkVal)
							}
							badRowsRes.Close()
							if hasBad {
								panic("Foreign Key Failure, see above.")
							}

							deferredStatements = append(deferredStatements, fmt.Sprintf(`ALTER TABLE %s 
								ADD CONSTRAINT fk_%s_%s_%s_%s 
								FOREIGN KEY (%s) 
								REFERENCES %s(%s)`, collectionName, collectionName, colName, refField.Collection, "id", colName, refField.Collection, "id"))
						}

					}
				}
			}
			for _, index := range indexes {
				if !index.Used && *index.ConstraintType == "FOREIGN KEY" {
					MustExecF(now, db, `
						ALTER TABLE %s DROP FOREIGN KEY %s`, collectionName, index.ConstraintName)
				}
			}
			for _, statement := range deferredStatements {
				MustExecF(now, db, statement)
			}

		} else {
			// CREATE!
			log.Println("CREATE TABLE")
			params := make([]string, 0, 0)

			for name, field := range collection.Fields {
				params = append(params, fmt.Sprintf("`%s` %s", name, field.GetMysqlDef()))
			}

			params = append(params, "PRIMARY KEY (`id`)")

			MustExecF(now, db, "CREATE TABLE %s (%s)", collectionName, strings.Join(params, ", "))
		}
		res.Close()
	}

	log.Println("==========")
	log.Printf("\n\n%s\n\n", execString)
}
