package databath

import (
	"fmt"
	"github.com/daemonl/go_lib/databath/types"
	"log"
	"strings"
)

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

func (f *FieldSetFieldDefTotalDuration) GetPath() string { return f.Path }

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
	mappedField, err := query.includeField(f.Path, &Field{Impl: &field}, f, mappedLinkCollection, &sel)

	sel = fmt.Sprintf("SUM(%s.%s - %s.%s)/(60*60) AS %s",
		mappedLinkCollection.alias,
		f.Stop,
		mappedLinkCollection.alias,
		f.Start,
		mappedField.alias)

	query.selectFields = append(query.selectFields, sel)
	return nil
}
