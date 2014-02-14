package databath

import (
	"log"
)

type FieldSetFieldDefAggregate struct {
	path      string `json:"path"`
	pathSplit string
}

func (f *FieldSetFieldDefAggregate) init() error {
	return nil
}

func (f *FieldSetFieldDefAggregate) GetPath() string { return f.path }

func (f *FieldSetFieldDefAggregate) walkField(query *Query, baseTable *MappedTable, index int) error {
	log.Printf("WalkField AGGREGATE \n")
	return nil
}

/*
  walkFieldAggregate: (baseTable, prefixPath, fieldDef)=>

    path = fieldDef.path.split(".")

    linkBaseTable = baseTable
    while baseTable.def.fields.hasOwnProperty(path[0])
      # This isn't the backref

      linkBaseTable = @leftJoin(baseTable, prefixPath, path)

    collectionName = path[0]
    collectionDef = @getCollectionDef(collectionName)
    collectionAlias = @includeCollection(collectionName, collectionName)

    collectionRef = baseTable.def.name
    linkBasePk = linkBaseTable.def.pk or "id"

    @joins.push "LEFT JOIN #{collectionDef.name} #{collectionAlias} on #{collectionAlias}.#{collectionRef} = #{linkBaseTable.alias}.#{linkBasePk} "

    fieldName = path[1]
    endFieldDef = collectionDef.fields[fieldName]
    #TODO: Make recursive AFTER backjoining.
    fieldAlias = @includeField(prefixPath.concat(path).join("."), endFieldDef, collectionAlias)
    @selectFields.push("#{fieldDef.ag_type}(#{collectionAlias}.#{fieldName}) AS #{fieldAlias}")
    null

*/
