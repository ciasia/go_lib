package types

import ()

//////////////
// PASSWORD //
//////////////
type FieldPassword struct{}

func (f *FieldPassword) GetMysqlDef() string { return "VARCHAR(512) NULL" }

func (f *FieldPassword) IsSearchable() bool { return false }

func (f *FieldPassword) Init(raw map[string]interface{}) error { return nil }

func (f *FieldPassword) FromDb(stored interface{}) (interface{}, error) {
	// String -> String
	return "*", nil
}
func (f *FieldPassword) ToDb(input interface{}) (string, error) {
	// String -> String
	inputString, ok := input.(string)
	if !ok {
		return "", MakeToDbUserErrorFromString("Must be a string")
	}

	return HashPassword(inputString), nil
}
func (f *FieldPassword) GetScanReciever() interface{} {
	var v string
	var vp *string = &v
	return &vp
}
