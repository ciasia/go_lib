package databath

import (
	"database/sql"
	"fmt"
	"github.com/daemonl/go_lib/databath/types"
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

func (q *Query) GetColNames() ([]string, error) {
	fieldSet, err := q.collection.GetFieldSet(q.conditions.fieldset)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(fieldSet), len(fieldSet))
	for i, fsfd := range fieldSet {
		names[i] = fsfd.GetPath()
	}
	return names, nil
}

func (q *Query) Dump() {
	log.Println("DUMP Field Map")
	for i, f := range q.map_field {
		log.Printf("K: %s A: %s P: %s\n", i, f.alias, f.fieldNameInTable)
	}
	log.Println("END Field Map")
}

func (q *Query) BuildSelect() (string, []interface{}, error) {
	rootIncludedTable, _ := q.includeCollection("", q.collection.TableName)

	allParameters := make([]interface{}, 0, 0)

	//log.Printf("==START Walk==")
	for _, fieldDef := range q.fieldList {
		//log.Printf("<w>")
		err := fieldDef.walkField(q, rootIncludedTable, 0)
		//log.Printf("</w>")
		if err != nil {
			log.Printf("Walk Error %s", err.Error())
			return "", allParameters, err
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
	whereString, whereParameters, err := q.makeWhereString(q.conditions)
	if err != nil {
		return "", allParameters, err
	}
	//log.Printf("==END Where==")
	pageString, err := q.makePageString(q.conditions)
	if err != nil {
		return "", allParameters, err
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

	return sql, whereParameters, nil
}

func (q *Query) BuildUpdate(changeset map[string]interface{}) (string, []interface{}, error) {

	allParameters := make([]interface{}, 0, 0)

	rootIncludedTable, _ := q.includeCollection("", q.collection.TableName)

	for _, fieldDef := range q.fieldList {
		err := fieldDef.walkField(q, rootIncludedTable, 0)
		if err != nil {
			return "", allParameters, err
		}
	}

	whereString, whereParameters, err := q.makeWhereString(q.conditions)
	if err != nil {
		return "", allParameters, UserErrorF("Building where conditions %s", err.Error())
	}

	updates := make([]string, 0, 0)
	updateParameters := make([]interface{}, 0, 0)
	for path, value := range changeset {
		field, ok := q.map_field[path]
		if !ok {
			q.Dump()

			return "", allParameters, UserErrorF("Attempt to update field not in fieldset: '%s'", path)
		}

		dbVal, err := field.field.ToDb(value, q.context)
		if err != nil {
			return "", allParameters, UserErrorF("Error converting %s to database value: %s", path, err.Error())
		}
		updateString := fmt.Sprintf("`%s`.`%s` = ?", field.table.alias, field.fieldNameInTable)

		updates = append(updates, updateString)
		if dbVal == "NULL" {
			updateParameters = append(updateParameters, nil)
		} else {
			updateParameters = append(updateParameters, dbVal)
		}
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
	allParameters = append(updateParameters, whereParameters...)

	return sql, allParameters, nil
}

// BuildInsert creates an INSERT INTO statement, the requested key,val is given as the first parameter
// Can only insert one table at a time.
func (q *Query) BuildInsert(valueMap map[string]interface{}) (string, []interface{}, error) {
	values := make([]string, 0, 0)
	fields := make([]string, 0, 0)
	queryParameters := make([]interface{}, 0, 0)

	rootIncludedTable, _ := q.includeCollection("", q.collection.TableName)
	for _, fieldDef := range q.fieldList {
		err := fieldDef.walkField(q, rootIncludedTable, 0)
		if err != nil {
			return "", queryParameters, err
		}
	}

	for path, value := range valueMap {
		field, ok := q.map_field[path]
		if !ok {
			q.Dump()
			return "", queryParameters, UserErrorF("Attempt to update field not in fieldset: '%s'", path)
		}
		dbValue, err := field.field.ToDb(value, q.context)
		if err != nil {
			return "", queryParameters, UserErrorF("Error converting %s to database value: %s", path, err.Error())
		}
		if field.table.collection != q.collection {
			return "", queryParameters, UserErrorF("Error using field in create command - field '%s' doesn't belong to root table", path)
		}
		fields = append(fields, field.fieldNameInTable)
		values = append(values, "?")

		if dbValue == "NULL" {
			queryParameters = append(queryParameters, nil)
		} else {
			queryParameters = append(queryParameters, dbValue)
		}
	}

	// Default Values
	for path, field := range q.collection.Fields {
		log.Printf("DEFAULT: %s.%s %v\n", q.collection.TableName, path, field.OnCreate)
		_, ok := valueMap[path]
		if ok {
			continue
		}
		dbValue, err := field.GetDefault(q.context)
		if err != nil {
			log.Printf("ERR in Default Value for '%s.%s' (%v): %s\n", q.collection.TableName, path, field.OnCreate, err.Error())
			continue
		}
		if len(dbValue) < 1 {
			continue
		}
		fields = append(fields, path)
		values = append(values, "?")
		queryParameters = append(queryParameters, dbValue)
	}

	sql := fmt.Sprintf("INSERT INTO `%s` (`%s`) VALUES (%s)",
		q.collection.TableName, strings.Join(fields, "`, `"), strings.Join(values, ", "))
	return sql, queryParameters, nil
}

func (q *Query) CheckDelete(db *sql.DB, id uint64) (*DeleteCheckResult, error) {
	return q.collection.CheckDelete(db, id)
}

func (q *Query) BuildDelete(id uint64) (string, error) {
	sql := fmt.Sprintf("DELETE FROM `%s` WHERE id=%d LIMIT 1", q.collection.TableName, id)
	return sql, nil
}

func (q *Query) RunQueryWithResults(bath *Bath, sqlString string, parameters []interface{}) ([]map[string]interface{}, error) {
	allRows := make([]map[string]interface{}, 0, 0)
	log.Printf("SQL: %s %#v", sqlString, parameters)
	c := bath.GetConnection()
	db := c.GetDB()
	defer c.Release()
	res, err := db.Query(sqlString, parameters...)
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

func (q *Query) RunQueryWithSingleResult(bath *Bath, sqlString string, parameters []interface{}) (map[string]interface{}, error) {
	allRows, err := q.RunQueryWithResults(bath, sqlString, parameters)
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

	alreadyMapped, ok := q.map_table[path]
	if ok {
		return alreadyMapped, nil
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

func (q *Query) includeField(fullName string, field *Field, mappedTable *MappedTable, selectString *string) (*MappedField, error) {
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
		AllowSearch:      true,
	}
	q.map_field[fullName] = &mf
	q.i_field += 1
	return &mf, nil
}

func (q *Query) leftJoin(baseTable *MappedTable, prefixPath []string, tableField string) (*MappedTable, error) {
	fieldDef, fieldExists := baseTable.collection.Fields[tableField]
	if !fieldExists {
		return nil, QueryUserError{"Field " + tableField + " does not exist in " + baseTable.collection.TableName}
	}
	tableIncludePath := strings.Join(prefixPath, ".") + "." + tableField
	refField := fieldDef.Impl.(*types.FieldRef)

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
