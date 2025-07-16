package model

import (
	"context"
)

type IDBRegistry interface {
	GetDB(ctx context.Context) (*DataDatabasesMap, error)
	Run()
	Stop()
	Ping() error
}
