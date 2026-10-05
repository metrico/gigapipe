//go:build integration

// ClickHouse is reached at CLICKHOUSE_HTTP_URL (default http://localhost:8123), database
// CLICKHOUSE_DB (default cloki).

package integration

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// envURL is the URL in env without a trailing slash, or def when env is unset.
func envURL(env, def string) string {
	if u := os.Getenv(env); u != "" {
		return strings.TrimRight(u, "/")
	}
	return def
}

func clickhouseURL() string {
	return envURL("CLICKHOUSE_HTTP_URL", "http://localhost:8123")
}

func clickhouseDB() string {
	if db := os.Getenv("CLICKHOUSE_DB"); db != "" {
		return db
	}
	return "cloki"
}

// clickhouseConn connects to the ClickHouse at CLICKHOUSE_HTTP_URL.
func clickhouseConn(t *testing.T) clickhouse.Conn {
	t.Helper()
	return clickhouseConnAt(t, clickhouseURL())
}

// clickhouseConnAt connects to the ClickHouse at base, whose user info, if any, authenticates.
func clickhouseConnAt(t *testing.T, base string) clickhouse.Conn {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	pass, _ := u.User.Password()
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{u.Host}, Protocol: clickhouse.HTTP,
		Auth:        clickhouse.Auth{Database: clickhouseDB(), Username: u.User.Username(), Password: pass},
		ReadTimeout: 5 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func clickhouseQuery(t *testing.T, sql string) string {
	t.Helper()
	return clickhouseQueryAt(t, clickhouseURL(), sql)
}

// clickhouseQueryAt runs sql on the ClickHouse at base, whose user info, if any, authenticates.
func clickhouseQueryAt(t *testing.T, base, sql string) string {
	t.Helper()
	resp, err := http.Post(strings.TrimRight(base, "/")+"/?database="+clickhouseDB(), "text/plain",
		strings.NewReader(sql))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("clickhouse status = %d, body = %q", resp.StatusCode, raw)
	}
	return strings.TrimSpace(string(raw))
}

// clickhouseQueryAs runs sql under queryID, ignoring its outcome.
func clickhouseQueryAs(queryID, sql string) {
	resp, err := http.Post(clickhouseURL()+"/?database="+clickhouseDB()+"&query_id="+url.QueryEscape(queryID),
		"text/plain", strings.NewReader(sql))
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}
