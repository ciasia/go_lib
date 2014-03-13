package databath

import (
	"log"
)

type FieldSetFieldDefNormal struct {
	path      string
	pathSplit []string
}

func (f *FieldSetFieldDefNormal) init() error {
	return nil
}

func (f *FieldSetFieldDefNormal) GetPath() string { return f.path }

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
		fieldAlias, _ := query.includeField(f.path, field, f, baseTable, nil)
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
