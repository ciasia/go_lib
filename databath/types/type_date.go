package types

import ()

type FieldDate struct{}

func (f *FieldDate) GetMysqlDef() string { return "DATE NULL" }

func (f *FieldDate) IsSearchable() bool { return false }

func (f *FieldDate) Init(raw map[string]interface{}) error { return nil }

func (f *FieldDate) FromDb(stored interface{}) (interface{}, error) {
	//

	storedString, ok := stored.(*string)
	if !ok {
		return nil, makeConversionError("date", stored)
	}

	if storedString == nil {
		return nil, nil
	}
	return *storedString, nil

}

func (f *FieldDate) ToDb(input interface{}) (string, error) {

	str, ok := input.(string)
	if !ok {
		return "", MakeToDbUserErrorFromString("Must be a string")
	}

	return str, nil
}

func (f *FieldDate) GetScanReciever() interface{} {
	var v string
	var vp *string = &v
	return &vp
}
