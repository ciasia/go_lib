package types

import (
	"fmt"
)

///////////
// FLOAT //
///////////
type FieldFloat struct{}

func (f *FieldFloat) GetMysqlDef() string { return "FLOAT NULL" }

func (f *FieldFloat) IsSearchable() bool { return false }

func (f *FieldFloat) Init(raw map[string]interface{}) error { return nil }

func (f *FieldFloat) FromDb(stored interface{}) (interface{}, error) {
	// float64 -> float64

	storedFloatPointer, ok := stored.(*float64)
	if !ok {
		return nil, makeConversionError("float64", stored)
	}

	if storedFloatPointer == nil {
		return nil, nil
	} else {
		return *storedFloatPointer, nil
	}
}
func (f *FieldFloat) ToDb(input interface{}) (string, error) {
	if input == nil {
		return "NULL", nil
	}
	inputInt, ok := input.(float64)
	if !ok {
		return "", MakeToDbUserErrorFromString("Must be a float")
	}

	return fmt.Sprintf("%f", inputInt), nil
}

func (f *FieldFloat) GetScanReciever() interface{} {
	var v float64
	var vp *float64 = &v
	return &vp
}
