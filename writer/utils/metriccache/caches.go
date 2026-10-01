package metriccache

import (
	"context"
	"sync"
	"time"
)

const (
	predecessorIdle = time.Hour
	resetEvery      = 30 * time.Minute
	evictEvery      = time.Minute
)

// Node is the metric caches of one database node.
type Node struct {
	Predecessors *Predecessors
	Fingerprints *Fingerprints
	Metadata     *Metadata
}

// Caches holds a Node per database node. The fingerprint and metadata caches
// are reset every resetEvery and idle predecessors are evicted every minute.
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
			Predecessors: NewPredecessors(predecessorIdle, nil),
			Fingerprints: NewFingerprints(),
			Metadata:     NewMetadata(),
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
	reset := time.NewTicker(resetEvery)
	evict := time.NewTicker(evictEvery)
	defer reset.Stop()
	defer evict.Stop()
	for {
		select {
		case <-reset.C:
			c.each(func(n *Node) {
				n.Fingerprints.Reset()
				n.Metadata.Reset()
			})
		case <-evict.C:
			c.each(func(n *Node) { n.Predecessors.EvictIdle() })
		case <-c.stop:
			return
		}
	}
}

type ctxKey struct{}

// NewContext returns ctx carrying the node's metric caches.
func NewContext(ctx context.Context, n *Node) context.Context {
	return context.WithValue(ctx, ctxKey{}, n)
}

// FromContext returns the metric caches ctx carries, or nil.
func FromContext(ctx context.Context) *Node {
	n, _ := ctx.Value(ctxKey{}).(*Node)
	return n
}
