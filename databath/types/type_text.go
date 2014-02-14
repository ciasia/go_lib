package types

import ()

//////////
// TEXT //
//////////
type FieldText struct{}

func (f *FieldText) GetMysqlDef() string { return "TEXT NULL" }

func (f *FieldText) IsSearchable() bool { return true }

func (f *FieldText) Init(raw map[string]interface{}) error { return nil }

func (f *FieldText) FromDb(stored interface{}) (interface{}, error) {
	// String -> String
	storedString, ok := stored.(*string)
	if !ok {
		return nil, MakeFromDbErrorFromString("Incorrect Type in DB (expected string)")
	}
	if storedString == nil {
		return nil, nil
	}
	return UnescapeString(*storedString), nil
}
func (f *FieldText) ToDb(input interface{}) (string, error) {
	// String -> String
	inputString, ok := input.(string)
	if !ok {
		return "", MakeToDbUserErrorFromString("Must be a string")
	}
	return EscapeString(inputString), nil
}
func (f *FieldText) GetScanReciever() interface{} {
	var v string
	var vp *string = &v
	return &vp
}
