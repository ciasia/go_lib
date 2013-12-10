package types

import (
	"fmt"
)

//////////
// FILE //
//////////
type FieldFile struct{}

func (f *FieldFile) GetMysqlDef() string { return "VARCHAR(255) NULL" }

func (f *FieldFile) IsSearchable() bool { return true }

func (f *FieldFile) Init(raw map[string]interface{}) error { return nil }

func (f *FieldFile) FromDb(stored interface{}) (interface{}, error) {
	// String -> String

	storedStringPointer, ok := stored.(*string)
	if !ok {
		return nil, makeConversionError("string", stored)
	}

	if storedStringPointer == nil {
		return nil, nil
	} else {
		return UnescapeString(*storedStringPointer), nil
	}
}

func (f *FieldFile) ToDb(input interface{}) (string, error) {
	// String -> String
	inputString, ok := input.(string)
	if !ok {
		return "", MakeToDbUserErrorFromString(fmt.Sprintf("Converting string to DB, Value Must be a string, got '%v'", input))
	}
	return EscapeString(inputString), nil
}
func (f *FieldFile) GetScanReciever() interface{} {
	var s string
	var sp *string = &s
	return &sp
}
