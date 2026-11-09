package registry

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/reader/utils/logger"
)

// pingLimit is one database's time limit in a health check.
const pingLimit = 30 * time.Second

type staticDBRegistry struct {
	databases    []*model.DataDatabasesMap
	mtx          sync.Mutex
	lastPingTime time.Time
}

var _ model.IDBRegistry = &staticDBRegistry{}

func NewStaticDBRegistry(databases map[string]*model.DataDatabasesMap) model.IDBRegistry {
	res := staticDBRegistry{
		lastPingTime: time.Now(),
	}
	for _, d := range databases {
		res.databases = append(res.databases, d)
	}
	return &res
}

func (s *staticDBRegistry) GetDB(ctx context.Context) (*model.DataDatabasesMap, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	idx := rand.IntN(len(s.databases))
	return s.databases[idx], nil
}

func (s *staticDBRegistry) Run() {
}

func (s *staticDBRegistry) Stop() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	logger.Info("Stopping staticDBRegistry, closing sessions...")
	for _, dbMap := range s.databases {
		if dbMap.Session != nil {
			dbMap.Session.Close()
		}
	}
	s.databases = nil
	s.lastPingTime = time.Time{}
}

func (s *staticDBRegistry) Ping() error {
	if s.lastPingTime.Add(time.Second * 30).After(time.Now()) {
		return nil
	}
	for _, v := range s.databases {
		if err := pingDB(v, pingLimit); err != nil {
			return err
		}
	}
	s.lastPingTime = time.Now()
	return nil
}

// pingDB takes a connection to v's database, without a limit, and pings on it
// for at most limit: under a deadline natively, under a cancel timer over HTTP.
func pingDB(v *model.DataDatabasesMap, limit time.Duration) error {
	conn, err := v.Session.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	limited := context.WithTimeout
	if v.Config.HttpPort != 0 {
		limited = dsn.WithLimit
	}
	to, cancel := limited(context.Background(), limit)
	defer cancel()
	return conn.PingContext(to)
}
