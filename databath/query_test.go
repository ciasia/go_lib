package databath

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
)

func TestQueryBuilder(t *testing.T) {
	jsonReader := getTestModelStream()
	model, err := ReadModelFromReader(jsonReader)
	if err != nil {
		t.Error(err)
	}

	context := MapContext{
		Fields: make(map[string]interface{}),
	}

	where1 := QueryConditionWhere{
		Field: "customer.name",
		Cmp:   "=",
		Val:   "44",
	}
	where2 := QueryConditionWhere{
		Field: "customer.name",
		Cmp:   "IN",
		Val:   []string{"1", "2", "3"},
	}

	fieldset := "table"
	conditions := QueryConditions{
		collection: "project",
		fieldset:   &fieldset,
		where:      make([]QueryCondition, 2, 2),
	}
	conditions.where[0] = &where1
	conditions.where[1] = &where2

	q, err := GetQuery(&context, model, &conditions)
	if err != nil {
		fmt.Println(err)
		return
	}
	s, err := q.BuildSelect()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(s)
}

func TestQueryParser(t *testing.T) {

	jsonReader := getTestModelStream()
	model, err := ReadModelFromReader(jsonReader)
	if err != nil {
		t.Error(err)
	}

	context := MapContext{
		Fields: make(map[string]interface{}),
	}

	jsonReader2 := getTestQueryStream()
	qp, err := ReadQueryFromReader(jsonReader2)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%v\n", qp)
	q, err := GetQuery(&context, model, qp)
	if err != nil {
		fmt.Println(err)
		return
	}
	s, err := q.BuildSelect()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(s)

}

func getTestQueryStream() io.ReadCloser {
	jsonBlob := `{
			"collection": "project",
			"fieldset": "table",
			"limit": 20,
			"offset": 0,
			"sort": [{"fieldName": "customer.name", "direction": 1}],
			"where": [{"field": "customer.name", "cmp": "=", "val": "33"}, {"field": "customer.name", "cmp": "IN", "val": ["1","2","3"]}],
			"search": {"name": "light", "customer.name": "jim"}
	}
	`

	r := bytes.NewReader([]byte(jsonBlob))
	rc := ioutil.NopCloser(r)

	return rc
}
