package types

import (
	"fmt"
)

// string
type FieldString struct {
	Length uint64
}

func (f *FieldString) GetMysqlDef() string {
	return fmt.Sprintf("VARCHAR(%d) NULL", f.Length)
}

func (f *FieldString) IsSearchable() bool { return true }

func (f *FieldString) Init(raw map[string]interface{}) error {
	err := mapValueDefaultUInt64(raw, "length", 1000, &f.Length)
	if err != nil {
		return err
	}
	return nil
}

func (f *FieldString) FromDb(stored interface{}) (interface{}, error) {
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

func (f *FieldString) ToDb(input interface{}) (string, error) {
	// String -> String
	inputString, ok := input.(string)
	if !ok {
		return "", MakeToDbUserErrorFromString(fmt.Sprintf("Converting string to DB, Value Must be a string, got '%v'", input))
	}
	return EscapeString(inputString), nil
}
func (f *FieldString) GetScanReciever() interface{} {
	var s string
	var sp *string = &s
	return &sp
}
