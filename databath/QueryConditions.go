package databath

import ()

type QueryConditions struct {
	collection string
	where      []QueryCondition
	pk         *uint64
	fieldset   *string
	limit      *int64
	filter     *map[string]interface{}
	offset     *int64
	sort       []*QuerySort
	search     map[string]string
}

func (qc *QueryConditions) CollectionName() string {
	return qc.collection
}

func (qc *QueryConditions) AndWhere(extraCondition QueryCondition) {
	qc.where = append(qc.where, extraCondition)
}

type QueryCondition interface {
	GetConditionString(q *Query) (string, []interface{}, bool, error)
}

type QuerySort struct {
	Direction int32  `json:"direction"`
	FieldName string `json:"fieldName"`
}

type QueryConditionString struct {
	Str        string // No JSON. This CANNOT be exposed to the user, Utility Only.
	Parameters []interface{}
}

func (qc *QueryConditionString) GetConditionString(q *Query) (string, []interface{}, bool, error) {
	return "(" + qc.Str + ")", qc.Parameters, false, nil
}

func GetMinimalQueryConditions(collectionName string, fieldset string) *QueryConditions {
	qc := QueryConditions{
		collection: collectionName,
		where:      make([]QueryCondition, 0, 0),
		fieldset:   &fieldset,
	}
	return &qc
}
