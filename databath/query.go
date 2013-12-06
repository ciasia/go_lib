package databath

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
)

type Query struct {
	collection   *Collection
	model        *Model
	fieldList    []FieldSetFieldDef
	conditions   *QueryConditions
	i_table      int32
	i_field      int32
	map_table    map[string]*MappedTable
	map_field    map[string]*MappedField
	selectFields []string
	joins        []string
	context      Context
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
	field            Field
	table            *MappedTable
	def              *Collection
	selectString     *string
}

type Context interface {
	getValueFor(string) interface{}
}

type MapContext struct {
	Fields map[string]interface{}
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

func GetQuery(context Context, model *Model, conditions *QueryConditions) (*Query, error) {
	collection, ok := model.Collections[conditions.collection]
	if !ok {
		return nil, QueryUserError{"No collection named " + conditions.collection}
	}

	fieldList, err := collection.GetFieldSet(conditions.fieldset)
	if err != nil {
		return nil, err
	}
	query := Query{
		context:    context,
		collection: collection,
		model:      model,
		fieldList:  fieldList,
		conditions: conditions,
		i_table:    0,
		i_field:    0,
		map_table:  make(map[string]*MappedTable),
		map_field:  make(map[string]*MappedField),
		joins:      make([]string, 0, 0),
	}
	return &query, nil
}

func (q *Query) Dump() {
	log.Println("DUMP Field Map")
	for i, f := range q.map_field {
		log.Printf("K: %s A: %s P: %s\n", i, f.alias, f.fieldNameInTable)
	}
	log.Println("END Field Map")
}

func (q *Query) BuildSelect() (string, error) {
	rootIncludedTable, _ := q.includeCollection("", q.collection.TableName)

	//log.Printf("==START Walk==")
	for _, fieldDef := range q.fieldList {
		//log.Printf("<w>")
		err := fieldDef.walkField(q, rootIncludedTable, 0)
		//log.Printf("</w>")
		if err != nil {
			log.Printf("Walk Error %s", err.Error())
			return "", err
		}
	}
	//log.Printf("==END Walk==")

	//log.Printf("==START Select==")
	selectFields := make([]string, len(q.map_field), len(q.map_field))
	i := 0

	for _, mappedField := range q.map_field {
		if mappedField.selectString == nil {
			selectFields[i] = fmt.Sprintf("%s.%s AS %s", mappedField.table.alias, mappedField.fieldNameInTable, mappedField.alias)
		} else {
			selectFields[i] = *mappedField.selectString
		}

		i++
	}

	//log.Printf("==END Select==")

	//log.Printf("==START Where==")
	whereString, err := q.makeWhereString(q.conditions)
	if err != nil {
		return "", err
	}
	//log.Printf("==END Where==")
	pageString, err := q.makePageString(q.conditions)
	if err != nil {
		return "", err
	}
	joinString := strings.Join(q.joins, "\n  ")
	sql := fmt.Sprintf(`
    SELECT %s FROM %s t0 
    %s
    %s 
    GROUP BY t0.id 
    %s 
    `,
		strings.Join(selectFields, ", "),
		q.collection.TableName,
		joinString,
		whereString,
		pageString)

	return sql, nil
}

func (q *Query) BuildUpdate(changeset map[string]interface{}) (string, error) {

	rootIncludedTable, _ := q.includeCollection("", q.collection.TableName)

	for _, fieldDef := range q.fieldList {
		err := fieldDef.walkField(q, rootIncludedTable, 0)
		if err != nil {
			return "", err
		}
	}

	whereString, err := q.makeWhereString(q.conditions)
	if err != nil {
		return "", UserErrorF("Building where conditions %s", err.Error())
	}

	updates := make([]string, 0, 0)

	for path, value := range changeset {
		field, ok := q.map_field[path]
		if !ok {
			q.Dump()

			return "", UserErrorF("Attempt to update field not in fieldset: '%s'", path)
		}

		escapedValue, err := field.field.ToDb(value)
		if err != nil {
			return "", UserErrorF("Error converting %s to database value: %s", path, err.Error())
		}
		updateString := fmt.Sprintf("%s.%s = %s", field.table.alias, field.fieldNameInTable, escapedValue)
		updates = append(updates, updateString)
	}
	limit := "LIMIT 1"
	joins := ""
	if q.conditions.limit != nil {
		if *q.conditions.limit > 0 {
			limit = fmt.Sprintf("LIMIT %d", *q.conditions.limit)
		} else {
			// This allows a '-1' to 'unlimit' the update
			limit = ""
			joins = strings.Join(q.joins, "\n  ")
			// That is: Joins only work without a limit, and the scenarios always line up... hopefully
		}

	}

	sql := fmt.Sprintf(`UPDATE %s %s %s SET %s %s %s`,
		rootIncludedTable.collection.TableName,
		rootIncludedTable.alias,
		joins,
		strings.Join(updates, ", "),
		whereString,
		limit)
	return sql, nil
}

func (q *Query) BuildInsert(parameters map[string]interface{}) (string, error) {
	values := make([]string, 0, 0)
	fields := make([]string, 0, 0)

	rootIncludedTable, _ := q.includeCollection("", q.collection.TableName)
	for _, fieldDef := range q.fieldList {
		err := fieldDef.walkField(q, rootIncludedTable, 0)
		if err != nil {
			return "", err
		}
	}

	for path, value := range parameters {
		field, ok := q.map_field[path]
		if !ok {
			q.Dump()
			return "", UserErrorF("Attempt to update field not in fieldset: '%s'", path)
		}
		escapedValue, err := field.field.ToDb(value)
		if err != nil {
			return "", UserErrorF("Error converting %s to database value: %s", path, err.Error())
		}
		if field.table.collection != q.collection {
			return "", UserErrorF("Error using field in create command - field '%s' doesn't belong to root table", path)
		}
		fields = append(fields, field.fieldNameInTable)
		values = append(values, escapedValue)
	}
	sql := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`,
		q.collection.TableName, strings.Join(fields, ", "), strings.Join(values, ", "))
	return sql, nil
}

func (q *Query) BuildDelete(id uint64) (string, error) {
	sql := fmt.Sprintf(`DELETE FROM %s WHERE id=%d LIMIT 1`, q.collection.TableName, id)
	return sql, nil
}

func (q *Query) RunQueryWithResults(bath *Bath, sqlString string) ([]map[string]interface{}, error) {
	allRows := make([]map[string]interface{}, 0, 0)
	log.Println("SQL: " + sqlString)
	c := bath.GetConnection()
	db := c.GetDB()
	defer c.Release()
	res, err := db.Query(sqlString)
	if err != nil {
		return allRows, err
	}

	for res.Next() {
		converted, err := q.ConvertResultRow(res)
		if err != nil {
			return allRows, err
		}

		allRows = append(allRows, converted)
	}
	return allRows, nil
}
func (q *Query) RunQueryWithSingleResult(bath *Bath, sqlString string) (map[string]interface{}, error) {
	allRows, err := q.RunQueryWithResults(bath, sqlString)
	if err != nil {
		return make(map[string]interface{}), err
	}
	if len(allRows) != 1 {
		return make(map[string]interface{}), UserErrorF("More than one result in single result query")
	}
	return allRows[0], nil
}

func (q *Query) ConvertResultRow(rs *sql.Rows) (map[string]interface{}, error) {
	// This is a mess...
	// Most important thing is the way pointer types are handled.
	// Scan needs a pointer to a pointer of the correct type. (Nullable requires pointer)
	// Creating a pointer to the result of a function with type interface{}
	// makes reflect see a pointer to an interface type, not the actual type returned by the function
	// so the type functions need to return a pointer to a pointer to the correct type.
	// Then type assertions in here make sure it is an ACTUAL pointer to a pointer to a [type]
	// and the scan function will see the correct type and fill it.

	// the 'rs.Columns()' map key is the alias, so spin a new map of map_field by alias name.
	aliasMap := make(map[string]interface{})

	for _, mappedField := range q.map_field {
		r := mappedField.field.GetScanReciever()
		// r is a pointer to a pointer of the correct type (**string, **float64 etc). (NOT a *interface{}, or **interface{} which are different things)
		aliasMap[mappedField.alias] = r
	}

	// Create the raw values array of **[type] in the correct order
	cols, _ := rs.Columns()
	rawValues := make([]interface{}, len(cols), len(cols))
	for i, colName := range cols {
		singlePointerValue := aliasMap[colName]
		rawValues[i] = singlePointerValue
	}

	// Scan the values - copies the row result into the value pointed by the 'rawValue'
	err := rs.Scan(rawValues...)
	if err != nil {
		return nil, err
	}

	// pathMap is the object to be JSONified and returned to the user.
	pathMap := make(map[string]interface{})

	// Pass the returned values through the field FromDb Method, and populate the map.
	for path, mappedField := range q.map_field {
		if aliasMap[mappedField.alias] != nil {
			val := aliasMap[mappedField.alias]
			rv := reflect.Indirect(reflect.ValueOf(val)).Interface()
			from, err := mappedField.field.FromDb(rv)
			if err != nil {
				return nil, err
			}
			pathMap[path] = from
		}
	}

	return pathMap, err
}

func (q *Query) includeCollection(path string, collectionName string) (*MappedTable, error) {

	collection, ok := q.model.Collections[collectionName]

	if !ok {
		return nil, QueryUserError{"Collection " + collectionName + " doesn't exist"}
	}

	alias := fmt.Sprintf("t%d", q.i_table)
	mt := MappedTable{
		alias:      alias,
		path:       path,
		collection: collection,
	}
	q.map_table[path] = &mt
	q.i_table += 1
	return &mt, nil

}

func (q *Query) includeField(fullName string, field Field, mappedTable *MappedTable, selectString *string) (*MappedField, error) {
	if field == nil {
		panic("Nil Field in includeField")
		//return nil, new QueryUserError{"Nil Field in includeField"}
	}
	alias := fmt.Sprintf("f%d", q.i_field)
	fieldParts := strings.Split(fullName, ".")
	fieldNameInTable := fieldParts[len(fieldParts)-1]

	mf := MappedField{
		alias:            alias,
		field:            field,
		fieldNameInTable: fieldNameInTable,
		table:            mappedTable,
		selectString:     selectString,
	}
	q.map_field[fullName] = &mf
	q.i_field += 1
	return &mf, nil
}

func (q *Query) leftJoin(baseTable *MappedTable, prefixPath []string, path []string) (*MappedTable, error) {
	tableField := path[0]
	path = path[1:]
	fieldDef, fieldExists := baseTable.collection.Fields[tableField]
	if !fieldExists {
		return nil, QueryUserError{"Field " + tableField + " does not exist in " + baseTable.collection.TableName}
	}
	prefixPath = append(prefixPath, tableField)
	tableIncludePath := strings.Join(prefixPath, ".")
	refField := fieldDef.(*FieldRef)

	existingDef, ok := q.map_table[tableIncludePath]
	if ok {
		return existingDef, nil
	} else {
		includedCollection, err := q.includeCollection(tableIncludePath, refField.Collection)
		if err != nil {
			return nil, err
		}
		q.joins = append(q.joins, fmt.Sprintf(
			`LEFT JOIN %s %s ON %s.id = %s.%s`,
			includedCollection.collection.TableName,
			includedCollection.alias,
			includedCollection.alias,
			baseTable.alias,
			tableField))
		return includedCollection, nil
	}
}
