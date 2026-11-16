package registry

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jmoiron/sqlx"
	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/utils/dsn"
	"github.com/metrico/qryn/v5/shared/chtest"
)

// httpAddrEnv names the HTTP address (host:port) of the chtest.AddrEnv server.
const httpAddrEnv = "GIGAPIPE_TEST_CLICKHOUSE_HTTP"

// dataDB is a database of the registry, connected with opts.
func dataDB(t *testing.T, opts *clickhouse.Options) *model.DataDatabasesMap {
	pool := sqlx.NewDb(clickhouse.OpenDB(opts), "clickhouse")
	t.Cleanup(func() { _ = pool.Close() })
	cfg := &config.ClokiBaseDataBase{}
	if opts.Protocol == clickhouse.HTTP {
		cfg.HttpPort = 8123
	}
	return &model.DataDatabasesMap{
		Config:  cfg,
		Session: &dsn.StableSqlxDBWrapper{DB: pool, GetDB: func() *sqlx.DB { return pool }},
	}
}

// The health check passes for a readonly=1 user.
func testPingAsReadonlyUser(t *testing.T, opts *clickhouse.Options) {
	conn, db := chtest.Open(t)
	ctx := context.Background()
	user := fmt.Sprintf("chtest_ping_%d", rand.Uint64())
	t.Cleanup(func() { _ = conn.Exec(ctx, "DROP USER IF EXISTS "+user) })
	if err := conn.Exec(ctx, "CREATE USER "+user+" IDENTIFIED WITH no_password SETTINGS readonly = 1"); err != nil {
		t.Fatal(err)
	}
	opts.Auth = clickhouse.Auth{Database: db, Username: user}
	registry := &staticDBRegistry{databases: []*model.DataDatabasesMap{dataDB(t, opts)}}
	if err := registry.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestPingAsReadonlyUser(t *testing.T) {
	testPingAsReadonlyUser(t, &clickhouse.Options{Addr: []string{os.Getenv(chtest.AddrEnv)}})
}

func TestPingAsReadonlyUserOverHTTP(t *testing.T) {
	addr := os.Getenv(httpAddrEnv)
	if addr == "" {
		t.Skipf("%s is not set", httpAddrEnv)
	}
	testPingAsReadonlyUser(t, &clickhouse.Options{Addr: []string{addr}, Protocol: clickhouse.HTTP})
}

// A ping of a server that stops answering ends at its limit, well before opts'
// read timeout.
func testPingEndsAtItsLimit(t *testing.T, opts *clickhouse.Options) {
	const limit = 200 * time.Millisecond
	opts.ReadTimeout = 20 * time.Second
	start := time.Now()
	err := pingDB(dataDB(t, opts), limit)
	if err == nil {
		t.Error("ping of a hung server passed")
	}
	if took := time.Since(start); took < limit || took >= opts.ReadTimeout/2 {
		t.Errorf("ping took %v, limit %v: %v", took, limit, err)
	}
}

// hungNativeServer completes the native handshake and then never answers.
func hungNativeServer(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	putString := func(b []byte, s string) []byte {
		return append(binary.AppendUvarint(b, uint64(len(s))), s...)
	}
	hello := putString([]byte{0}, "hung")
	for _, v := range []uint64{25, 3, 54032} { // major, minor, revision
		hello = binary.AppendUvarint(hello, v)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = conn.Write(hello)
				_, _ = io.Copy(io.Discard, conn)
			}()
		}
	}()
	return l.Addr().String()
}

func TestPingEndsAtItsLimit(t *testing.T) {
	testPingEndsAtItsLimit(t, &clickhouse.Options{Addr: []string{hungNativeServer(t)}})
}

// hangingHTTPProxy forwards to addr but never answers a ping.
func hangingHTTPProxy(t *testing.T, addr string) string {
	proxy := httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: addr})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		if strings.TrimSpace(string(body)) == "SELECT 1" {
			<-r.Context().Done()
			return
		}
		r.Body = io.NopCloser(strings.NewReader(string(body)))
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

func TestPingEndsAtItsLimitOverHTTP(t *testing.T) {
	addr := os.Getenv(httpAddrEnv)
	if addr == "" {
		t.Skipf("%s is not set", httpAddrEnv)
	}
	testPingEndsAtItsLimit(t, &clickhouse.Options{Addr: []string{hangingHTTPProxy(t, addr)}, Protocol: clickhouse.HTTP})
}
