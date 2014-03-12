package types

import (
	"fmt"
)

// string
type FieldEnum struct {
	Length  uint64
	Choices map[string]string
}

func (f *FieldEnum) GetMysqlDef() string {
	return fmt.Sprintf("VARCHAR(%d) NULL", f.Length)
}

func (f *FieldEnum) IsSearchable() bool { return true }

func (f *FieldEnum) Init(raw map[string]interface{}) error {
	err := mapValueDefaultUInt64(raw, "length", 1000, &f.Length)
	if err != nil {
		return err
	}

	choices, ok := raw["choices"].(map[string]interface{})
	if !ok {
		return UserErrorF("No choices key, %#v", raw)
	}

	f.Choices = make(map[string]string)
	for k, v := range choices {
		f.Choices[k] = fmt.Sprintf("%v", v)
	}
	return nil
}

func (f *FieldEnum) FromDb(stored interface{}) (interface{}, error) {
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

func (f *FieldEnum) ToDb(input interface{}) (string, error) {
	// String -> String
	inputString, ok := input.(string)
	if !ok {
		return "", MakeToDbUserErrorFromString(fmt.Sprintf("Converting string to DB, Value Must be a string, got '%v'", input))
	}
	return EscapeString(inputString), nil
}
func (f *FieldEnum) GetScanReciever() interface{} {
	var s string
	var sp *string = &s
	return &sp
}
