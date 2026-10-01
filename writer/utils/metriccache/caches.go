package metriccache

import (
	"sync"
	"time"
)

const (
	PredecessorIdle  = time.Hour
	SeriesResetEvery = 30 * time.Minute
	evictEvery       = time.Minute
)

// Node is the metric caches of one database node.
type Node struct {
	Predecessors *Predecessors
	Series       *Series
}

// Caches holds a Node per database node. The series caches are reset every
// SeriesResetEvery and idle predecessors are evicted every minute.
type Caches struct {
	mtx   sync.Mutex
	nodes map[string]*Node
	stop  chan struct{}
	once  sync.Once
}

func New() *Caches {
	c := &Caches{nodes: make(map[string]*Node), stop: make(chan struct{})}
	go c.run()
	return c
}

// Node returns the caches of the named database node, creating them on first use.
func (c *Caches) Node(name string) *Node {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	n, ok := c.nodes[name]
	if !ok {
		n = &Node{
			Predecessors: NewPredecessors(PredecessorIdle, nil),
			Series:       NewSeries(),
		}
		c.nodes[name] = n
	}
	return n
}

func (c *Caches) Stop() {
	c.once.Do(func() { close(c.stop) })
}

func (c *Caches) each(fn func(n *Node)) {
	c.mtx.Lock()
	nodes := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mtx.Unlock()
	for _, n := range nodes {
		fn(n)
	}
}

func (c *Caches) run() {
	reset := time.NewTicker(SeriesResetEvery)
	evict := time.NewTicker(evictEvery)
	defer reset.Stop()
	defer evict.Stop()
	for {
		select {
		case <-reset.C:
			c.each(func(n *Node) { n.Series.Reset() })
		case <-evict.C:
			c.each(func(n *Node) { n.Predecessors.EvictIdle() })
		case <-c.stop:
			return
		}
	}
}
