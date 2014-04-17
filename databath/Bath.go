package databath

import (
	"database/sql"
	//"log"
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

func (bath *Bath) GetConnection() *Connection {
	c := <-bath.conn
	//log.Println("GET CONNECTION")
	return c
}

func (bath *Bath) ReleaseConnection(c *Connection) {
	bath.conn <- c
}

type Connection struct {
	db   *sql.DB
	bath *Bath
}

func (c *Connection) GetDB() *sql.DB {
	return c.db
}

func (c *Connection) Release() {
	//log.Println("RELEASE CONNECTION")
	c.bath.ReleaseConnection(c)
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
