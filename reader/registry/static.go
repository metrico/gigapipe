package registry

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/metrico/qryn/v4/reader/model"
)

type staticDBRegistry struct {
	databases    []*model.DataDatabasesMap
	rand         *rand.Rand
	mtx          sync.Mutex
	lastPingTime time.Time
}

var _ model.IDBRegistry = &staticDBRegistry{}

func NewStaticDBRegistry(databases map[string]*model.DataDatabasesMap) model.IDBRegistry {
	res := staticDBRegistry{
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
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
	idx := s.rand.Intn(len(s.databases))
	return s.databases[idx], nil
}

func (s *staticDBRegistry) Run() {
}

func (s *staticDBRegistry) Stop() {
}

func (s *staticDBRegistry) Ping() error {
	if s.lastPingTime.Add(time.Second * 30).After(time.Now()) {
		return nil
	}
	for _, v := range s.databases {
		err := func(db model.ISqlxDB) error {
			conn, err := v.Session.Conn(context.Background())
			if err != nil {
				return err
			}
			defer conn.Close()
			to, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()
			return conn.PingContext(to)
		}(v.Session)
		if err != nil {
			return err
		}
	}
	s.lastPingTime = time.Now()
	return nil
}
