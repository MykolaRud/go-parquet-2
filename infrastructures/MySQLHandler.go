package infrastructures

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"log"
)

func NewMySQLHandler(connection *sql.DB) *MySQLHandler {
	return &MySQLHandler{connection}
}

func InitDBConnection(MySQLConfig mysql.Config) *sql.DB {
	var err error

	Conn, err := sql.Open("mysql", MySQLConfig.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}

	pingErr := Conn.Ping()
	if pingErr != nil {
		log.Fatal(pingErr)
	}
	fmt.Println("Connected!")

	return Conn
}

type MySQLHandler struct {
	Conn *sql.DB
}

func (handler *MySQLHandler) SetConn(Connection *sql.DB) {
	handler.Conn = Connection
}

func (handler *MySQLHandler) Execute(statement string, args ...any) (sql.Result, error) {

	return handler.Conn.Exec(statement, args...)
}

func (handler *MySQLHandler) Query(statement string, args ...any) (*sql.Rows, error) {
	rows, err := handler.Conn.Query(statement, args...)

	if err != nil {
		fmt.Println(err)
		return rows, err
	}

	return rows, nil
}

func (handler *MySQLHandler) QueryRow(statement string, args ...any) *sql.Row {
	row := handler.Conn.QueryRow(statement, args...)

	return row
}
