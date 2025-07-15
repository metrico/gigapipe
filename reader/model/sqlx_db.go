package model

import (
	"context"
	"database/sql"
)

type ISqlxDB interface {
	GetName() string
	/*Query(query string, args ...any) (*sql.Rows, error)*/
	QueryCtx(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecCtx(ctx context.Context, query string, args ...any) error
	Conn(ctx context.Context) (*sql.Conn, error)
	Begin() (*sql.Tx, error)
	Close()
}
