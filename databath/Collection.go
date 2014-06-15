package databath

import (
	"database/sql"
	"fmt"
	"github.com/daemonl/go_lib/databath/types"
	"log"
	"strings"
)

type Collection struct {
	Model          *Model
	Fields         map[string]*Field
	FieldSets      map[string][]FieldSetFieldDef
	CustomFields   map[string]FieldSetFieldDef
	Masks          map[uint64]*Mask
	ForeignKeys    []*Field
	Hooks          []Hook
	TableName      string
	SearchPrefixes map[string]*SearchPrefix
}

func (c *Collection) GetFieldSet(fieldSetNamePointer *string) ([]FieldSetFieldDef, error) {
	var fieldSetName string
	if fieldSetNamePointer == nil {
		fieldSetName = "default"
	} else {
		fieldSetName = *fieldSetNamePointer
	}

	fields, ok := c.FieldSets[fieldSetName]
	if !ok {
		return nil, QueryUserError{"Fieldset " + fieldSetName + " doesn't exist in " + c.TableName}
	}
	log.Printf("Using fieldset: %s.%s\n", c.TableName, fieldSetName)

	return fields, nil
}

func (c *Collection) CheckDelete(db *sql.DB, id uint64) (*DeleteCheckResult, error) {

	dd := &DeleteCheckResult{
		Children:           make(map[string]map[string]*DeleteCheckResult),
		ToExecute:          make([]string, 0, 0),
		WillBeDeleted:      make(map[string][]uint64),
		PreventingDeletion: make(map[string][]uint64),
		Prevents:           false,
	}

	for _, field := range c.ForeignKeys {
		refField, ok := field.Impl.(*types.FieldRef)
		if !ok {
			return nil, ParseErrF("Foreign key not a ref type")
		}

		sql := fmt.Sprintf("SELECT id FROM `%s` WHERE  %s.%s = %d", field.Collection.TableName, field.Collection.TableName, field.Path, id)
		log.Printf("CHECK DELETE: %s\n", sql)
		res, err := db.Query(sql)
		if err != nil {
			return nil, err
		}
		defer res.Close()
		existingRefs := make([]uint64, 0, 0)
		for res.Next() {
			var refId uint64
			res.Scan(&refId)
			existingRefs = append(existingRefs, refId)
			if refField.OnDelete == types.RefOnDeleteCascade {
				dd.WillBeDeleted[field.Collection.TableName] = existingRefs
				refDeleteCheckResult, err := c.Model.Collections[field.Collection.TableName].CheckDelete(db, refId)
				if err != nil {
					return nil, err
				}
				dd.ToExecute = append(dd.ToExecute, fmt.Sprintf("DELETE FROM `%s` WHERE id = %d", field.Collection.TableName, refId))
				dd.AddChild(refDeleteCheckResult, field.Collection.TableName, refId)

			} else if refField.OnDelete == types.RefOnDeleteNull {
				dd.ToExecute = append(dd.ToExecute, fmt.Sprintf("UPDATE %s SET %s = NULL WHERE id = %d", field.Collection.TableName, field.Path, refId))

			} else {
				dd.PreventingDeletion[field.Collection.TableName] = existingRefs
				dd.Prevents = true
			}
		}
	}

	return dd, nil
}

type DeleteCheckResult struct {
	ToExecute          []string                                 `json:"-"`
	WillBeDeleted      map[string][]uint64                      `json:"willBeDeleted"`
	PreventingDeletion map[string][]uint64                      `json:"preventsDeletion"`
	Children           map[string]map[string]*DeleteCheckResult `json:"children"`
	Prevents           bool                                     `json:"preventsDeletion"`
}

func (dcr *DeleteCheckResult) AddChild(childDcr *DeleteCheckResult, childCollection string, childId uint64) {
	child, ok := dcr.Children[childCollection]
	if !ok {
		child = make(map[string]*DeleteCheckResult)
		dcr.Children[childCollection] = child
	}

	childIdStr := fmt.Sprintf("%d", childId)

	child[childIdStr] = childDcr

	if childDcr.Prevents {
		dcr.Prevents = true
	}
}

func (dcr *DeleteCheckResult) GetIssues() []string {
	strs := make([]string, 0, 0)
	for collectionName, ids := range dcr.PreventingDeletion {
		idStrings := make([]string, len(ids), len(ids))
		for i, idInt := range ids {
			idStrings[i] = fmt.Sprintf("%d", idInt)
		}
		strs = append(strs, fmt.Sprintf("%s: %s", collectionName, strings.Join(idStrings, ", ")))
	}
	for collectionName, children := range dcr.Children {
		for id, child := range children {
			for _, str := range child.GetIssues() {
				strs = append(strs, collectionName+"["+id+"]."+str)
			}
		}
	}
	return strs
}

func (dcr *DeleteCheckResult) ExecuteRecursive(db *sql.DB) error {

	for _, children := range dcr.Children {
		for _, child := range children {
			err := child.ExecuteRecursive(db)
			if err != nil {
				return err
			}
		}
	}

	for _, sql := range dcr.ToExecute {
		_, err := db.Exec(sql)
		if err != nil {
			return err
		}
	}

	return nil
}

type SearchPrefix struct {
	Prefix    string
	Field     Field
	FieldName string
}
