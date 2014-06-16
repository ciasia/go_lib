package databath

import (
	"fmt"
	"log"
	"strings"
)

type FieldSetFieldDefRaw struct {
	Query    string  `json:"query"`
	DataType string  `json:"dataType"`
	Path     string  `json:"path"`
	Join     *string `json:"join"`
	SearchOn *string `json:"searchOn"`
}

func (f *FieldSetFieldDefRaw) init() error { return nil }

func (f *FieldSetFieldDefRaw) GetPath() string { return f.Path }

func (f *FieldSetFieldDefRaw) walkField(query *Query, baseTable *MappedTable, index int) error {

	fieldType, err := FieldByType(f.DataType)
	if err != nil {
		return err
	}

	sel := ""
	mappedField, err := query.includeField(f.Path, &Field{Impl: fieldType}, f, baseTable, &sel)
	mappedField.AllowSearch = false

	var replError error
	replFunc := func(in string) string {
		parts := strings.Split(in[1:len(in)-1], ".")
		currentTable := baseTable
		for i, _ := range parts[:len(parts)-1] {

			currentTable, err = query.leftJoin(currentTable, parts[:i+1], parts[i])
			if err != nil {
				replError = err
				return ""
			}

		}
		return currentTable.alias + "." + parts[len(parts)-1]
	}

	joinReplFunc := func(in string) string {

		collectionName := in[1 : len(in)-1]
		mapped, ok := query.map_table[collectionName]
		if ok {

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
