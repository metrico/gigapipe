package metriccache

import (
	"sync"

	"github.com/metrico/qryn/v5/writer/utils/metadata"
)

// Metadata is the metadata cache: each family's last written type, help and
// unit, until the next Reset.
type Metadata struct {
	mtx      sync.Mutex
	families map[string]metadata.Entry
}

func NewMetadata() *Metadata {
	return &Metadata{families: make(map[string]metadata.Entry)}
}

// Changed reports whether m differs from the family's recorded metadata, and
// records it.
func (c *Metadata) Changed(name string, m metadata.Entry) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if old, ok := c.families[name]; ok && old == m {
		return false
	}
	c.families[name] = m
	return true
}

func (c *Metadata) Reset() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.families = make(map[string]metadata.Entry)
}
