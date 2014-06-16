package databath

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
)

type FieldSetFieldDef interface {
	walkField(q *Query, baseTable *MappedTable, currentIndex int) error
	GetPath() string
	init() error
}

func getFieldSetFieldDef(name string, raw interface{}) (FieldSetFieldDef, error) {

	stringVal, isString := raw.(string)
	if isString {
		fsfd := FieldSetFieldDefNormal{
			path:      stringVal,
			pathSplit: strings.Split(stringVal, "."),
		}
		return &fsfd, nil
	}

	mapVals, isMap := raw.(map[string]interface{})
	if isMap {
		fdTypeRaw, ok := mapVals["type"]
		if !ok {
			return nil, fmt.Errorf("Field %s was a map without a 'type' key, couldn't be resolved", name)
		}
		fdType, ok := fdTypeRaw.(string)
		if !ok {
			return nil, fmt.Errorf("Field %s had non string 'type' key", name)
		}
		var fsfd FieldSetFieldDef
		switch fdType {
		case "totalduration":
			fsfdv := FieldSetFieldDefTotalDuration{}
			fsfd = &fsfdv
		case "aggregate":
			fsfdv := FieldSetFieldDefAggregate{}
			fsfd = &fsfdv
		case "raw":
			fsfdv := FieldSetFieldDefRaw{}
			fsfd = &fsfdv
		default:
			return nil, errors.New("Fieldset type " + fdType + " couldn't be resolved")
		}

		mapVals["path"] = name

		fsfdVal := reflect.Indirect(reflect.ValueOf(fsfd)).Type()
		fsfdElem := reflect.ValueOf(fsfd).Elem()
		var field reflect.StructField

		// Loop through the fields on the struct

		for i := 0; i < fsfdVal.NumField(); i++ {
			field = fsfdVal.Field(i) // reflect.StructField
			//log.Printf("FIELD TYPE: %v %v", field.Type, field.Type.Kind())

			if tag := field.Tag.Get("json"); tag != "" && tag != "-" {
				// The field has a json tag.
				mapVal, mapValExists := mapVals[tag]

				fieldVal := fsfdElem.FieldByIndex(field.Index) // reflect.Value

				isPointer := field.Type.Kind() == reflect.Ptr

				fieldType := field.Type

				mvv := reflect.ValueOf(mapVal)

				if isPointer {
					if mapValExists {
						// TODO: This only works with strings...
						var p string = reflect.ValueOf(mapVal).String()
						p = mvv.String()
						fieldVal.Set(reflect.ValueOf(&p))
					}
				} else {

					mvType := reflect.TypeOf(mapVal)

					if !mapValExists {
						return nil, errors.New("Fieldset type " + fdType + " couldn't be mapped, required map key '" + tag + "' not set")
					} else {
						if !mvType.AssignableTo(fieldType) {
							return nil, errors.New("Fieldset type " + fdType + " couldn't be mapped, map key '" + tag + "' not assignable to required type " + fieldVal.Type().String())
						}

						if fieldVal.CanSet() {
							if isPointer {
								log.Println("SET POINTER")
							}
							fieldVal.Set(mvv)
						} else {
							log.Println("Can't Set " + tag)
						}

					}
				}

			}
		}

		err := fsfd.init()
		return fsfd, err

	}

	fmt.Printf("FST: %v\n", raw)
	return nil, errors.New("Fieldset type couldn't be resolved")
}
