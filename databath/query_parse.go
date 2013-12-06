package databath

import (
	"encoding/json"
	"io"
)

type RawQueryConditions struct {
	Collection *string                 `json:"collection"`
	Fieldset   *string                 `json:"fieldset"`
	Limit      *int64                  `json:"limit"`
	Offset     *int64                  `json:"offset"`
	Where      []*QueryConditionWhere  `json:"where"`
	Sort       []*QuerySort            `json:"sort"`
	Filter     *map[string]interface{} `json:"filter"`
	Search     map[string]string       `json:"search"`
	Pk         *uint64                 `json:"pk"`
}

func (rawQuery *RawQueryConditions) TranslateToQuery() (*QueryConditions, error) {
	if rawQuery.Collection == nil {
		return nil, QueryUserError{"query must have a 'collection' key"}
	}

	where := make([]QueryCondition, len(rawQuery.Where), len(rawQuery.Where))
	for i, c := range rawQuery.Where {
		where[i] = c
	}
	conditions := QueryConditions{
		where:      where,
		fieldset:   rawQuery.Fieldset,
		collection: *rawQuery.Collection,
		filter:     rawQuery.Filter,
		sort:       rawQuery.Sort,
		search:     rawQuery.Search,
		limit:      rawQuery.Limit,
		offset:     rawQuery.Offset,
		pk:         rawQuery.Pk,
	}
	return &conditions, nil
}

func ReadQueryFromReader(reader io.ReadCloser) (*QueryConditions, error) {
	var rawQuery RawQueryConditions

	decoder := json.NewDecoder(reader)
	err := decoder.Decode(&rawQuery)
	if err != nil {
		return nil, err
	}
	return rawQuery.TranslateToQuery()

}

/*
"collection": "project",
"fieldset": "table",
"limit": 20,
"offset": 0,
"sort": ["customer.name"],
"where": [{"field": "customer.id", "cmp": "=", "val": "33"}, {"field": "customer.id", "cmp": "IN", "val": [1,2,3]}],
"search": {"name": "light", "customer.name": "jim"}
*/
