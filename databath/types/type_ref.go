package types

import ()

type FieldRef struct {
	Collection string
	OnDelete   int
	Limit      map[string]interface{}
}

const (
	RefOnDeletePrevent = iota
	RefOnDeleteCascade
	RefOnDeleteNull
)

func (f *FieldRef) GetMysqlDef() string { return "INT(11) UNSIGNED NULL" }

func (f *FieldRef) IsSearchable() bool { return false }

func (f *FieldRef) Init(raw map[string]interface{}) error {
	collection, ok := raw["collection"]
	if !ok {
		return ModelDefinitionError{"Ref Field has no key 'collection'", ""}
	}
	collectionString := collection.(string)
	f.Collection = collectionString

	onDelete, ok := raw["onDelete"]
	if ok {
		if onDelete == "CASCADE" {
			f.OnDelete = RefOnDeleteCascade
		} else if onDelete == "NULL" {
			f.OnDelete = RefOnDeleteNull
		}
	} else {
		f.OnDelete = RefOnDeletePrevent
	}

	f.Limit = map[string]interface{}{}

	rawLimit, ok := raw["limit"]
	if ok {
		l, ok := rawLimit.(map[string]interface{})
		if ok {
			f.Limit = l
		}
	}
	return nil
}

func (f *FieldRef) FromDb(stored interface{}) (interface{}, error) {
	// uInt64 -> Iunt64
	storedInt, ok := stored.(*uint64)
	if !ok {
		return nil, MakeFromDbErrorFromString("Incorrect Type in DB (expected uint64)")
	}
	if storedInt == nil {
		return nil, nil
	}
	return *storedInt, nil
}

func (f *FieldRef) ToDb(input interface{}) (string, error) {
	return unsignedIntToDb(input)
}

func (f *FieldRef) GetScanReciever() interface{} {
	var v uint64
	var vp *uint64 = &v
	return &vp
}
