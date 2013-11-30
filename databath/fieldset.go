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
		default:
			return nil, errors.New("Fieldset type " + fdType + " couldn't be resolved")
		}

		fsfdVal := reflect.Indirect(reflect.ValueOf(fsfd)).Type()
		fsfdElem := reflect.ValueOf(fsfd).Elem()
		var field reflect.StructField

		// Loop through the fields on the struct
		for i := 0; i < fsfdVal.NumField(); i++ {
			field = fsfdVal.Field(i)

			if tag := field.Tag.Get("json"); tag != "" {
				// The field has a json tag.

				mapVal, mapValExists := mapVals[tag]
				mvType := reflect.TypeOf(mapVal)
				if !mapValExists {
					return nil, errors.New("Fieldset type " + fdType + " couldn't be mapped, required map key '" + tag + "' not set")
				}

				if !mvType.AssignableTo(field.Type) {
					return nil, errors.New("Fieldset type " + fdType + " couldn't be mapped, map key '" + tag + "' not assignable to required type")
				}
				fieldVal := fsfdElem.Field(i)
				if fieldVal.CanSet() {
					fieldVal.Set(reflect.ValueOf(mapVal))
				}
			}
		}

		err := fsfd.init()
		return fsfd, err

	}

	fmt.Printf("FST: %v\n", raw)
	return nil, errors.New("Fieldset type couldn't be resolved")

	/*
		if reflect.TypeOf(raw).Kind() == reflect.Map {

			stringMap := make(map[string]interface{})

			rawMap := reflect.ValueOf(raw)

			fieldSetFieldType, ok := rawMap.MapIndex("type")
			if !ok {

			}


			return nil, errors.New("Fieldset type " + fieldSetFieldType + " couldn't be resolved")

		}


	*/
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
		log.Printf("Field Doesn't exist\n")
		return nil
	}
	if field == nil {
		log.Printf("Field Is Null\n")
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

		newTable, err := query.leftJoin(baseTable, f.pathSplit[0:index], f.pathSplit[index:])
		if err != nil {
			return err
		}
		//log.Printf("RECURSE")
		return f.walkField(query, newTable, index+1)

	}
}

type FieldSetFieldDefAggregate struct {
	path      string `json:"path"`
	pathSplit string `json:"path"`
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
			linkBaseTable, err = query.leftJoin(linkBaseTable, f.PathSplit[:i], f.PathSplit[i+1:])
			if err != nil {
				return err
			}
		} else {
			break
		}
	}

	mappedLinkCollection, err := query.includeCollection(strings.Join(f.PathSplit[:basePathIndex], "."), linkCollectionName)
	if err != nil {
		return err
	}

	join := fmt.Sprintf("LEFT JOIN %s %s ON %s.%s = %s.id",
		mappedLinkCollection.collection.TableName,
		mappedLinkCollection.alias,
		mappedLinkCollection.alias,
		baseTable.collection.TableName,
		baseTable.alias)
	query.joins = append(query.joins, join)

	field := FieldFloat{}

	// ODD OBSUCIRY: sel requires knowledge of the return from includeField.
	// The pointer will only be used after this function returns
	// Possible race condition?
	sel := ""
	mappedField, err := query.includeField(f.Path, &field, mappedLinkCollection, &sel)

	sel = fmt.Sprintf("SUM(%s.%s - %s.%s) AS %s",
		mappedLinkCollection.alias,
		f.Stop,
		mappedLinkCollection.alias,
		f.Start,
		mappedField.alias)

	query.selectFields = append(query.selectFields, sel)
	return nil
}
