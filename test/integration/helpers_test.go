//go:build integration

// ClickHouse is reached at CLICKHOUSE_HTTP_URL (default http://localhost:8123), database
// CLICKHOUSE_DB (default cloki).

package integration

import (
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
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
