package interfaces

import "database/sql"

type IDbHandler interface {
	SetConn(Connection *sql.DB)
	Execute(statement string, args ...any) (sql.Result, error)
	Query(statement string, args ...any) (*sql.Rows, error)
	QueryRow(statement string, args ...any) *sql.Row
}

type IRow interface {
	Scan(dest ...interface{}) error
	Next() bool
}
