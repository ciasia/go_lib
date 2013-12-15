package databath

import (
	"errors"
	"fmt"
	"github.com/daemonl/go_lib/databath/types"
	"log"
	"reflect"
	"strings"
)

type FieldSetFieldDef interface {
	walkField(q *Query, baseTable *MappedTable, currentIndex int) error
	init() error
}

func getFieldSetFieldDef(raw interface{}) (FieldSetFieldDef, error) {

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
			return nil, errors.New("Fieldset was a map without a 'type' key, couldn't be resolved")
		}
		fdType, ok := fdTypeRaw.(string)
		if !ok {
			return nil, errors.New("Fieldset had non string 'type' key")
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

		fsfdVal := reflect.Indirect(reflect.ValueOf(fsfd)).Type()
		fsfdElem := reflect.ValueOf(fsfd).Elem()
		var field reflect.StructField

		// Loop through the fields on the struct

		for i := 0; i < fsfdVal.NumField(); i++ {
			field = fsfdVal.Field(i) // reflect.StructField
			log.Printf("FIELD TYPE: %v %v", field.Type, field.Type.Kind())

			if tag := field.Tag.Get("json"); tag != "" {
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

type FieldSetFieldDefNormal struct {
	path      string
	pathSplit []string
}

func (f *FieldSetFieldDefNormal) init() error {
	return nil
}
func (f *FieldSetFieldDefNormal) walkField(query *Query, baseTable *MappedTable, index int) error {

	if index >= len(f.pathSplit) {
		return nil
	}

	fieldName := f.pathSplit[index]
	//log.Printf("WalkField fieldName: %s\n", fieldName)

	field, fieldExists := baseTable.collection.Fields[fieldName]
	if !fieldExists {
		log.Printf("Field %s Doesn't exist\n", fieldName)
		return nil
	}
	if field == nil {
		log.Printf("Field %a Is Null\n", fieldName)
		return nil
	}

	if index == len(f.pathSplit)-1 {
		// Then this is the last part of a a.b.c, so in the query it appears: "[table b's alias].c AS [field c's alias]"
		//log.Printf("LAST PART %s", strings.Join(f.pathSplit, "."))
		fieldAlias, _ := query.includeField(f.path, field, baseTable, nil)
		_ = fieldAlias
		return nil
	} else {
		// Otherwise, include a new table (If needed)

		newTable, err := query.leftJoin(baseTable, f.pathSplit[0:index], f.pathSplit[index])
		if err != nil {
			return err
		}
		//log.Printf("RECURSE")
		return f.walkField(query, newTable, index+1)

	}
}

/////////
// RAW //
/////////

type FieldSetFieldDefRaw struct {
	Query    string  `json:"query"`
	DataType string  `json:"dataType"`
	Path     string  `json:"path"`
	Join     *string `json:"join"`
}

func (f *FieldSetFieldDefRaw) init() error { return nil }
func (f *FieldSetFieldDefRaw) walkField(query *Query, baseTable *MappedTable, index int) error {

	field, err := FieldByType(f.DataType)
	if err != nil {
		return err
	}

	sel := ""
	mappedField, err := query.includeField(f.Path, field, baseTable, &sel)
	mappedField.AllowSearch = false

	var replError error
	replFunc := func(in string) string {
		log.Println("Walk: " + in)
		parts := strings.Split(in[1:len(in)-1], ".")
		currentTable := baseTable
		for i, tableJump := range parts[:len(parts)-1] {
			log.Println("Walk " + tableJump)
			currentTable, err = query.leftJoin(currentTable, parts[:i+1], parts[i])
			if err != nil {
				replError = err
				return ""
			}

		}
		return currentTable.alias + "." + parts[len(parts)-1]
	}

	joinReplFunc := func(in string) string {
		log.Println("Collection Walk: " + in)
		collectionName := in[1 : len(in)-1]
		mapped, ok := query.map_table[collectionName]
		if ok {
			log.Printf("Alias: %s->%s\n", collectionName, mapped.alias)
			return mapped.alias

		}
		fmt.Println(query.map_table)
		log.Printf("No Alias: %s\n", collectionName)

		return collectionName
	}

	if f.Join != nil {
		joinReplaced := re_fieldInSquares.ReplaceAllStringFunc(*f.Join, joinReplFunc)
		query.joins = append(query.joins, joinReplaced)
	}

	raw := re_fieldInSquares.ReplaceAllStringFunc(f.Query, replFunc)

	if replError != nil {
		return replError
	}

	sel = raw + " AS " + mappedField.alias

	return nil
}

type FieldSetFieldDefAggregate struct {
	path      string `json:"path"`
	pathSplit string
}

func (f *FieldSetFieldDefAggregate) init() error {
	return nil
}
func (f *FieldSetFieldDefAggregate) walkField(query *Query, baseTable *MappedTable, index int) error {
	log.Printf("WalkField AGGREGATE \n")
	return nil
}

/*
  walkFieldAggregate: (baseTable, prefixPath, fieldDef)=>

    path = fieldDef.path.split(".")

    linkBaseTable = baseTable
    while baseTable.def.fields.hasOwnProperty(path[0])
      # This isn't the backref

      linkBaseTable = @leftJoin(baseTable, prefixPath, path)

    collectionName = path[0]
    collectionDef = @getCollectionDef(collectionName)
    collectionAlias = @includeCollection(collectionName, collectionName)

    collectionRef = baseTable.def.name
    linkBasePk = linkBaseTable.def.pk or "id"

    @joins.push "LEFT JOIN #{collectionDef.name} #{collectionAlias} on #{collectionAlias}.#{collectionRef} = #{linkBaseTable.alias}.#{linkBasePk} "

    fieldName = path[1]
    endFieldDef = collectionDef.fields[fieldName]
    #TODO: Make recursive AFTER backjoining.
    fieldAlias = @includeField(prefixPath.concat(path).join("."), endFieldDef, collectionAlias)
    @selectFields.push("#{fieldDef.ag_type}(#{collectionAlias}.#{fieldName}) AS #{fieldAlias}")
    null

*/

// Warning: The following struct name looks like Java.
type FieldSetFieldDefTotalDuration struct {
	Path      string `json:"path"`
	PathSplit []string
	Label     string `json:"label"`
	DataType  string `json:"dataType"`
	Start     string `json:"start"`
	Stop      string `json:"stop"`
}

func (f *FieldSetFieldDefTotalDuration) init() error {
	f.PathSplit = strings.Split(f.Path, ".")

	return nil
}

func (f *FieldSetFieldDefTotalDuration) walkField(query *Query, baseTable *MappedTable, index int) error {
	var err error

	linkBaseTable := baseTable

	linkCollectionName := ""
	basePathIndex := 0
	for i, part := range f.PathSplit {
		linkCollectionName = part
		basePathIndex = i
		_, ok := linkBaseTable.collection.Fields[part]
		if ok {

			linkBaseTable, err = query.leftJoin(linkBaseTable, f.PathSplit[:i], f.PathSplit[i+1])
			if err != nil {
				return err
			}
		} else {
			break
		}
	}

	mappedLinkCollection, err := query.includeCollection("R:"+strings.Join(f.PathSplit[:basePathIndex], "."), linkCollectionName)
	if err != nil {
		return err
	}

	log.Println("WALK ")

	join := fmt.Sprintf("LEFT JOIN %s %s ON %s.%s = %s.id",
		mappedLinkCollection.collection.TableName,
		mappedLinkCollection.alias,
		mappedLinkCollection.alias,
		baseTable.collection.TableName,
		baseTable.alias)
	query.joins = append(query.joins, join)

	field := types.FieldFloat{}

	// ODD OBSUCIRY: sel requires knowledge of the return from includeField.
	// The pointer will only be used after this function returns
	// Possible race condition?
	sel := ""
	mappedField, err := query.includeField(f.Path, &field, mappedLinkCollection, &sel)

	sel = fmt.Sprintf("SUM(%s.%s - %s.%s)/(60*60) AS %s",
		mappedLinkCollection.alias,
		f.Stop,
		mappedLinkCollection.alias,
		f.Start,
		mappedField.alias)

	query.selectFields = append(query.selectFields, sel)
	return nil
}
