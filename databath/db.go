package databath

import (
	"database/sql"
)

type Bath struct {
	DriverName       string
	ConnectionString string
	conn             chan *Connection
}

func (bath *Bath) Connect() (*sql.DB, error) {
	db, err := sql.Open(bath.DriverName, bath.ConnectionString)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

type Tenant struct {
}

type User struct {
	Tenant *Tenant
}

type Connection struct {
	db   *sql.DB
	bath *Bath
}

func (c *Connection) Release() {
	c.bath.ReleaseConnection(c)
}
func (bath *Bath) ReleaseConnection(c *Connection) {
	bath.conn <- c
}

func RunABath(driverName string, connectionString string, size int) *Bath {

	bath := Bath{
		DriverName:       driverName,
		ConnectionString: connectionString,
		conn:             make(chan *Connection, size),
	}

	for x := 0; x < size; x++ {
		db, err := bath.Connect()
		if err != nil {
			panic(err)
		}
		conn := Connection{
			db:   db,
			bath: &bath,
		}

		bath.conn <- &conn
	}
	return &bath
}

func (bath *Bath) GetConnection() *Connection {
	c := <-bath.conn
	return c
}

func (c *Connection) GetDB() *sql.DB {
	return c.db
}
