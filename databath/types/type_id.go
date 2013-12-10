package types

import ()

type FieldId struct{}

func (f *FieldId) GetMysqlDef() string { return "INT(11) UNSIGNED NOT NULL AUTO_INCREMENT" }

func (f *FieldId) IsSearchable() bool { return false }

func (f *FieldId) Init(raw map[string]interface{}) error { return nil }

func (f *FieldId) FromDb(stored interface{}) (interface{}, error) {
	// uInt64 -> uInt64
	storedInt, ok := stored.(*uint64)
	if !ok {
		return nil, MakeFromDbErrorFromString("Incorrect Type in DB (expected uint64)")
	}
	if storedInt == nil {
		return nil, nil
	}
	return *storedInt, nil
}
func (f *FieldId) ToDb(input interface{}) (string, error) {
	// uInt64 -> uInt64
	return unsignedIntToDb(input)
}
func (f *FieldId) GetScanReciever() interface{} {
	var v uint64
	var vp *uint64 = &v
	return &vp
}
