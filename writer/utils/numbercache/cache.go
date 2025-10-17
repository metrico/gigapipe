package numbercache

import (
	"context"
	"sync"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/metrico/qryn/v4/writer/model"
)

type ICache[T any] interface {
	CheckAndSet(key T) bool
	DB(db string) ICache[T]
	Stop()
}

type Cache[K any] struct {
	nodeMap       map[string]*model.DataDatabasesMap
	sets          *fastcache.Cache
	mtx           *sync.Mutex
	db            []byte
	isDistributed bool
	ctx           context.Context
	cancel        context.CancelFunc
	serializer    func(t K) []byte
}

func (c *Cache[T]) CheckAndSet(key T) bool {
	if c.isDistributed {
		return false
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	k := append(c.db, c.serializer(key)...)
	if c.sets.Has(k) {
		return true
	}
	c.sets.Set(k, []byte{1})
	return false
}

func (c *Cache[T]) Stop() {
	c.cancel()
}

func (c *Cache[T]) DB(db string) ICache[T] {
	return &Cache[T]{
		isDistributed: c.nodeMap[db].ClusterName != "",
		nodeMap:       c.nodeMap,
		sets:          c.sets,
		mtx:           c.mtx,
		db:            []byte(db),
		serializer:    c.serializer,
	}
}

func NewCache[T comparable](TTL time.Duration, serializer func(val T) []byte,
	nodeMap map[string]*model.DataDatabasesMap,
) *Cache[T] {
	if serializer == nil {
		panic("NO SER")
	}
	res := &Cache[T]{
		nodeMap:    nodeMap,
		sets:       fastcache.New(100 * 1024 * 1024),
		mtx:        &sync.Mutex{},
		serializer: serializer,
	}
	res.ctx, res.cancel = context.WithCancel(context.Background())
	cleanup := time.NewTicker(TTL)
	go func() {
		for {
			select {
			case <-cleanup.C:
				res.mtx.Lock()
				res.sets.Reset()
				res.mtx.Unlock()
			case <-res.ctx.Done():
				cleanup.Stop()
				return
			}
		}
	}()
	return res
}
