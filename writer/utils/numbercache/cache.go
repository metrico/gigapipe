package numbercache

import (
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
	cleanup       *time.Ticker
	sets          *fastcache.Cache
	mtx           *sync.Mutex
	db            []byte
	isDistributed bool
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
	c.cleanup.Stop()
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
	res := Cache[T]{
		nodeMap:    nodeMap,
		cleanup:    time.NewTicker(TTL),
		sets:       fastcache.New(100 * 1024 * 1024),
		mtx:        &sync.Mutex{},
		serializer: serializer,
	}
	go func() {
		for range res.cleanup.C {
			res.mtx.Lock()
			res.sets.Reset()
			res.mtx.Unlock()
		}
	}()
	return &res
}
