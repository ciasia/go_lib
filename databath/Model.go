package databath

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

type Model struct {
	Collections      map[string]*Collection
	CustomQueries    map[string]*CustomQuery
	DynamicFunctions map[string]*DynamicFunction
}

func (m *Model) GetIdentityString(db *sql.DB, collectionName string, pk uint64) (string, error) {
	fs := "identity"
	var lim int64 = 1
	qc := QueryConditions{
		collection: collectionName,
		fieldset:   &fs,
		pk:         &pk,
		limit:      &lim,
	}
	context := MapContext{
		IsApplication: true,
	}
	q, err := GetQuery(&context, m, &qc, false)
	if err != nil {
		log.Println(err)
		return "", err
	}
	sql, parameters, err := q.BuildSelect()
	if err != nil {
		log.Println(err)
		return "", err
	}
	res, err := q.RunQueryWithSingleResult(db, sql, parameters)
	if err != nil {
		log.Println(err)
		return "", err
	}
	allParts := make([]string, 0, 0)
	for path, field := range res {
		if path != "id" &&
			path != "sortIndex" &&
			len(path) > 0 && !strings.HasSuffix(path, ".id") {
			allParts = append(allParts, fmt.Sprintf("%v", field))
		}
	}
	return strings.Join(allParts, ", "), nil
}
