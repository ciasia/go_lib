package databath

import (
	"fmt"
	"log"
	"strings"
	"time"
)

func (m *Model) GetIdentityString(bath *Bath, collectionName string, pk uint64) (string, error) {
	fs := "identity"
	var lim int64 = 1
	qc := QueryConditions{
		collection: collectionName,
		fieldset:   &fs,
		pk:         &pk,
		limit:      &lim,
	}
	context := MapContext{}
	q, err := GetQuery(&context, m, &qc)
	if err != nil {
		log.Println(err)
		return "", err
	}
	sql, parameters, err := q.BuildSelect()
	if err != nil {
		log.Println(err)
		return "", err
	}
	res, err := q.RunQueryWithSingleResult(bath, sql, parameters)
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

func (m *Model) WriteHistory(bath *Bath, userId uint64, action string, collectionName string, entityId uint64) {
	identity, _ := m.GetIdentityString(bath, collectionName, entityId)
	timestamp := time.Now().Unix()

	sql := fmt.Sprintf(`INSERT INTO history 
		(user, identity, timestamp, action, entity, entity_id) VALUES 
		(%d, '%s', %d, '%s', '%s', %d)`,
		userId, identity, timestamp, action, collectionName, entityId)
	//log.Println(sql)
	c := bath.GetConnection()
	db := c.GetDB()
	defer c.Release()
	_, err := db.Exec(sql)
	if err != nil {
		log.Println(err)
		return
	}
}
