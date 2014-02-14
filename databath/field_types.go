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

func FieldByType(typeString string) (Field, error) {
	switch typeString {
	case "string":
		return &types.FieldString{}, nil
	case "id":
		return &types.FieldId{}, nil
	case "ref":
		return &types.FieldRef{}, nil
	case "array":
		return &types.FieldString{}, nil
	case "datetime":
		return &types.FieldDateTime{}, nil
	case "date":
		return &types.FieldDate{}, nil
	case "int":
		return &types.FieldInt{}, nil
	case "bool":
		return &types.FieldBool{}, nil
	case "text":
		return &types.FieldText{}, nil
	case "address":
		return &types.FieldText{}, nil
	case "float":
		return &types.FieldFloat{}, nil
	case "password":
		return &types.FieldPassword{}, nil
	case "file":
		return &types.FieldFile{}, nil
	case "enum":
		return &types.FieldString{}, nil
	case "auto_timestamp":
		return &types.FieldInt{}, nil
	case "timestamp":
		return &types.FieldTimestamp{}, nil

	default:
		return nil, errors.New(fmt.Sprintf("Invalid Field Type '%s'", typeString))
	}
}

func FieldFromDef(rawField map[string]interface{}) (Field, error) {

	// field must have type
	fieldType, err := getFieldParamString(rawField, "type")
	if err != nil {
		return nil, errors.New(fmt.Sprintf("%s", err.Error()))
	}
	if fieldType == nil {
		return nil, errors.New(fmt.Sprintf("no type specified"))
	}
	field, err := FieldByType(*fieldType)
	if err != nil {
		return nil, err
	}
	field.Init(rawField)

	return field, nil
}
