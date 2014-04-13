package databath

import (
	"fmt"
)

type DynamicFunction struct {
	Filename   string   `json:"filename"`
	Parameters []string `json:"parameters"`
}

type MappedTable struct {
	path       string
	alias      string
	collection *Collection
}

type MappedField struct {
	path             string
	alias            string
	fieldNameInTable string
	fieldSetFieldDef FieldSetFieldDef
	field            *Field
	table            *MappedTable
	def              *Collection
	selectString     *string
	AllowSearch      bool
}

func (mf *MappedField) CanSearch() bool {
	if !mf.field.IsSearchable() {
		return false
	}
	return mf.AllowSearch
}

type Context interface {
	getValueFor(string) interface{}
	getUserLevel() (isApplication bool, userAccessLevel uint64)
}

type MapContext struct {
	IsApplication   bool
	UserAccessLevel uint64
	Fields          map[string]interface{}
}

func (mc *MapContext) getUserLevel() (bool, uint64) {
	return mc.IsApplication, mc.UserAccessLevel
}

func (mc *MapContext) getValueFor(key string) interface{} {
	val, ok := mc.Fields[key]
	if !ok {
		return key
	}
	return val
}

type QueryUserError struct {
	Message string
}

func (ue QueryUserError) Error() string {
	return ue.Message
}

func UserErrorF(format string, params ...interface{}) QueryUserError {
	return QueryUserError{fmt.Sprintf(format, params...)}
}
