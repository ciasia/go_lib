package types

import ()

//////////
// bool //
//////////
type FieldBool struct{}

func (f *FieldBool) GetMysqlDef() string { return "TINYINT(1) NOT NULL" }

func (f *FieldBool) IsSearchable() bool { return false }

func (f *FieldBool) Init(raw map[string]interface{}) error { return nil }

func (f *FieldBool) FromDb(stored interface{}) (interface{}, error) {
	storedBool, ok := stored.(*bool)
	if !ok {
		return nil, MakeFromDbErrorFromString("Incorrect Type in DB (expecting bool)")
	}
	if storedBool == nil {
		return nil, nil
	}
	return *storedBool, nil
}

func (f *FieldBool) ToDb(input interface{}) (string, error) {
	switch input := input.(type) {
	case bool:
		if input {
			return "1", nil
		} else {
			return "0", nil
		}

	default:
		return "", makeConversionError("bool", input)
	}
}

func (f *FieldBool) GetScanReciever() interface{} {
	var v bool
	var vp *bool = &v
	return &vp
}
