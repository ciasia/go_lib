package databath

import (
	"errors"
	"fmt"
	"github.com/daemonl/go_lib/databath/types"
)

type Field interface {
	Init(map[string]interface{}) error
	FromDb(interface{}) (interface{}, error)
	ToDb(interface{}) (string, error)
	GetScanReciever() interface{}
	IsSearchable() bool
	GetMysqlDef() string
}

func FieldFromDef(rawField map[string]interface{}) (Field, error) {
	var field Field

	// field must have type
	fieldType, err := getFieldParamString(rawField, "type")
	if err != nil {
		return nil, errors.New(fmt.Sprintf("%s", err.Error()))
	}
	if fieldType == nil {
		return nil, errors.New(fmt.Sprintf("no type specified"))
	}
	switch *fieldType {
	case "string":
		field = &types.FieldString{}
		field.Init(rawField)
	case "id":
		field = &types.FieldId{}
		field.Init(rawField)
	case "ref":
		field = &types.FieldRef{}
		field.Init(rawField)
	case "array":
		field = &types.FieldString{}
		field.Init(rawField)
	case "datetime":
		field = &types.FieldInt{}
		field.Init(rawField)
	case "date":
		field = &types.FieldDate{}
		field.Init(rawField)
	case "int":
		field = &types.FieldInt{}
		field.Init(rawField)
	case "bool":
		field = &types.FieldBool{}
		field.Init(rawField)
	case "text":
		field = &types.FieldText{}
		field.Init(rawField)
	case "address":
		field = &types.FieldText{}
		field.Init(rawField)
	case "float":
		field = &types.FieldFloat{}
		field.Init(rawField)
	case "password":
		field = &types.FieldPassword{}
		field.Init(rawField)
	case "file":
		field = &types.FieldFile{}
		field.Init(rawField)
	case "enum":
		field = &types.FieldString{}
		field.Init(rawField)
	case "auto_timestamp":
		field = &types.FieldInt{}
		field.Init(rawField)

	default:
		return nil, errors.New(fmt.Sprintf("Invalid Field Type '%s'", *fieldType))
	}
	return field, nil
}
