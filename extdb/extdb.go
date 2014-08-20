package extdb

import (
	"database/sql"
	"fmt"
	"reflect"

	_ "github.com/go-sql-driver/mysql"
)

func Open(connectionString string) (*DB, error) {
	dbRaw, err := sql.Open("mysql", connectionString)
	if err != nil {
		return nil, err
	}
	return &DB{raw: dbRaw}, nil
}

func WrapDB(dbRaw *sql.DB) *DB {
	return &DB{raw: dbRaw}
}

type DB struct {
	raw *sql.DB
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.raw.Query(query, args)
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.raw.QueryRow(query, args)
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.raw.Exec(query, args)
}

func (db *DB) Select(dest interface{}, query string, args ...interface{}) error {

	rows, err := db.raw.Query(query, args...)
	if err != nil {
		return err
	}

	dVal := reflect.ValueOf(dest).Elem()
	rowType := reflect.TypeOf(dest).Elem().Elem().Elem()

	for rows.Next() {
		rowVal := reflect.New(rowType)
		var rowInterface interface{} = rowVal.Interface()

		err := db.scanRow(rows, rowInterface)
		if err != nil {
			return err
		}

		dVal.Set(reflect.Append(dVal, rowVal))
	}
	return nil
}

func (db *DB) Get(dest interface{}, query string, args ...interface{}) error {
	rows, err := db.raw.Query(query, args...)
	if err != nil {
		return err
	}
	if !rows.Next() {
		return &NotFoundErr{}
	}
	return db.scanRow(rows, dest)
}

func (db *DB) scanRow(rows *sql.Rows, dest interface{}) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	pointers, err := db.getPointerArray(cols, dest)
	if err != nil {
		return err
	}

	err = rows.Scan(pointers...)

	if err != nil {
		return err
	}

	return nil

}

func (db *DB) getPointerArray(cols []string, dest interface{}) ([]interface{}, error) {
	// dest should be a pointer to a struct
	destVal := reflect.ValueOf(dest)
	destType := reflect.TypeOf(dest)

	if destType.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("Not pointer type")
	}
	destVal = destVal.Elem()
	destType = destVal.Type()
	if destType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Not struct type")
	}
	fieldsByTag := map[string]reflect.Value{}

	lenFields := destType.NumField()
	for i := 0; i < lenFields; i++ {
		fieldType := destType.FieldByIndex([]int{i})
		tag := fieldType.Tag.Get("db")
		if len(tag) < 1 || tag == "-" {
			continue
		}
		fieldsByTag[tag] = destVal.FieldByIndex([]int{i})
	}

	pointers := make([]interface{}, len(cols), len(cols))
	for i, col := range cols {
		field, ok := fieldsByTag[col]
		if !ok {
			return nil, fmt.Errorf("No field %s in struct %s", col, destType.Name())
		}
		pointers[i] = field.Addr().Interface()
	}

	return pointers, nil
}