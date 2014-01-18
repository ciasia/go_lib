package sync

import (
	"database/sql"
	"fmt"
	"github.com/daemonl/go_lib/databath"
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
	Name            string
	Engine          string
	Version         *uint32
	Row_format      *string
	Rows            *uint64
	Avg_row_length  *uint64
	Data_length     *uint64
	Max_data_length *uint64
	Index_length    *uint64
	Data_free       *uint64
	Auto_increment  *uint64
	Create_time     *string
	Update_time     *string
	Check_time      *string
	Collation       *string
	Checksum        *string
	Create_options  *string
	Comment         *string
}

type Column struct {
	Field   string
	Type    string
	Null    string
	Key     *string
	Default *string
	Extra   *string
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

func ScanToStruct(res *sql.Rows, obj interface{}) error {

	rv := reflect.ValueOf(obj)
	rt := reflect.TypeOf(obj)

	if reflect.Indirect(rv).Kind().String() != "struct" {
		panic("KIND NOT STRUCT" + rv.Kind().String())
	}

	valueElm := rv.Elem()

	maxElements := rt.Elem().NumField()
	scanVals := make([]interface{}, maxElements, maxElements)
	for i := 0; i < maxElements; i++ {

		interf := valueElm.Field(i).Addr().Interface()
		scanVals[i] = interf
	}
	err := res.Scan(scanVals...)
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
		err := ScanToStruct(res, &table)
		doErr(err)
		MustExecF(now, db, "ALTER TABLE %s ENGINE = 'InnoDB'", table.Name)
	}
	res.Close()

	for name, collection := range model.Collections {
		log.Printf("COLLECTION: %s\n", name)
		res, err := db.Query(`SHOW TABLE STATUS WHERE Name = ?`, name)
		doErr(err)
		if res.Next() {
			log.Println("UPDATE TABLE")
			// UPDATE!

			for colName, field := range collection.Fields {
				showRes, err := db.Query(`SHOW COLUMNS FROM `+name+` WHERE Field = ?`, colName)

				doErr(err)
				if showRes.Next() {
					col := Column{}
					err := ScanToStruct(showRes, &col)
					doErr(err)
					colStr := col.GetString()
					modelStr := field.GetMysqlDef()
					if colStr != modelStr {
						log.Printf("'%s' '%s'\n", colStr, modelStr)
						MustExecF(now, db, "ALTER TABLE %s CHANGE COLUMN %s %s %s",
							name, colName, colName, modelStr)
					}
				} else {
					MustExecF(now, db, "ALTER TABLE %s ADD `%s` %s", name, colName, field.GetMysqlDef())
				}
				showRes.Close()
			}

		} else {
			// CRAETE!
			log.Println("CRAETE TABLE")
			params := make([]string, 0, 0)

			for name, field := range collection.Fields {
				params = append(params, fmt.Sprintf("`%s` %s", name, field.GetMysqlDef()))
			}

			params = append(params, "PRIMARY KEY (`id`)")

			MustExecF(now, db, "CREATE TABLE %s (%s)", name, strings.Join(params, ", "))
		}
		res.Close()
	}
	fmt.Println("==========")
	fmt.Println(execString)
}
