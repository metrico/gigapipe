package service

import (
	"github.com/ClickHouse/ch-go/proto"
)

type IColPoolRes interface {
	ReleaseColPoolRes()
	Input() proto.InputColumn
	Size() int
	Reset()
}

type PooledColumn[T proto.ColInput] struct {
	Data      T
	Name      string
	onRelease func(res *PooledColumn[T])
	size      func(res *PooledColumn[T]) int
}

func (c *PooledColumn[T]) Input() proto.InputColumn {
	return proto.InputColumn{
		Name: c.Name,
		Data: c.Data,
	}
}

func (c *PooledColumn[T]) Value() T {
	return c.Data
}

func (c *PooledColumn[T]) ReleaseColPoolRes() {
	/*res := c.resource
	defer func() {
		if res.CreationTime().Add(time.Minute * 5).Before(time.Now()) {
			res.Destroy()
		} else {
			res.Release()
		}
	}()*/
	if c.onRelease != nil {
		c.onRelease(c)
	}
	c.onRelease = nil
	c.size = nil
}

func (c *PooledColumn[T]) Reset() {
	if c.onRelease != nil {
		c.onRelease(c)
	}
}

func (c *PooledColumn[T]) Size() int {
	if c.size != nil {
		return c.size(c)
	}
	return 0
}

type colPool[T proto.ColInput] struct {
	pool      func() *PooledColumn[T]
	onRelease func(res *PooledColumn[T])
	size      func(res *PooledColumn[T]) int
}

const defaultColPoolSize = 1500

func newColPool[T proto.ColInput](create func() T, size int32) *colPool[T] {
	if size == 0 {
		size = defaultColPoolSize
	}
	return &colPool[T]{
		pool: func() *PooledColumn[T] {
			return &PooledColumn[T]{
				Name: "",
				Data: create(),
			}
		},
	}
}

func (c *colPool[T]) Acquire(name string) *PooledColumn[T] {
	res := c.pool()
	res.Name = name
	res.onRelease = c.onRelease
	res.size = c.size
	return res
}

func (c *colPool[T]) OnRelease(fn func(column *PooledColumn[T])) *colPool[T] {
	c.onRelease = fn
	return c
}

func (c *colPool[T]) OnGetSize(fn func(column *PooledColumn[T]) int) *colPool[T] {
	c.size = fn
	return c
}

// Define a new type for pooled array
type pooledArray[T proto.ColInput] struct {
	Create func() []T
	Data   []T
}

// Implement iColPoolRes interface for pooled array
func (p *pooledArray[T]) ReleaseColPoolRes() {
	p.Data = p.Data[:0]
}

func (p *pooledArray[T]) Input() proto.InputColumn {
	// Implement as needed
	return proto.InputColumn{}
}

func (p *pooledArray[T]) Size() int {
	return len(p.Data)
}

func (p *pooledArray[T]) Reset() {
	p.Data = p.Data[:0]
}

func (c *colPool[T]) AcquireArray(name string) *pooledArray[T] {
	return &pooledArray[T]{
		Create: func() []T { return make([]T, 0, defaultColPoolSize) },
	}
}
