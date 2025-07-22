package numbercache

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/fastcache"
)

type DynamicCache[K any] struct {
	cleanup       *time.Ticker
	sets          *fastcache.Cache
	mtx           *sync.Mutex
	db            []byte
	isDistributed bool
	serializer    func(t K) []byte
}

func (c *DynamicCache[T]) CheckAndSet(key T) bool {
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

func (c *DynamicCache[T]) Stop() {
	c.cleanup.Stop()
}

func (c *DynamicCache[T]) DB(db string) ICache[T] {
	return &DynamicCache[T]{
		isDistributed: db[:2] == "c-",
		sets:          c.sets,
		mtx:           c.mtx,
		db:            []byte(db),
		serializer:    c.serializer,
	}
}
