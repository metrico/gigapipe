package service

import (
	"context"

	"github.com/ClickHouse/ch-go"
)

type IChClient interface {
	Ping(ctx context.Context) error
	Do(ctx context.Context, query ch.Query) error
	IsAsyncInsert() bool
	Close() error
	GetDSN() string
}

type IChClientFactory func() (IChClient, error)

type InsertSvcMap = map[string]IInsertServiceV2
