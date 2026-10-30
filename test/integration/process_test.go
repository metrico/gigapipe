//go:build integration

// Helpers for the integration tests that start their own gigapipe process
// against the ClickHouse the stack uses, each with a database of its own.
//
//	GIGAPIPE_BIN          gigapipe binary (default: the one `make build` writes)
//	CLICKHOUSE_SERVER     ClickHouse host for that process (default localhost)
//	CLICKHOUSE_PORT       ClickHouse native port (default 9000)
//	CLICKHOUSE_HTTP_URL   ClickHouse HTTP interface the tests query (default http://localhost:8123)

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// gigapipeBinary returns the binary to start, failing with a hint when it has
// not been built.
func gigapipeBinary(t *testing.T) string {
	t.Helper()
	bin := envOr("GIGAPIPE_BIN", filepath.Join("..", "..", "gigapipe"))
	abs, err := filepath.Abs(bin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(abs); err != nil {
		t.Fatalf("gigapipe binary %s not found (run `make build` at the repository root, or set GIGAPIPE_BIN): %v", abs, err)
	}
	return abs
}

// processEnv is the environment of a gigapipe process using database db.
func processEnv(db string, extra ...string) []string {
	return append([]string{
		"CLICKHOUSE_SERVER=" + envOr("CLICKHOUSE_SERVER", "localhost"),
		"CLICKHOUSE_PORT=" + envOr("CLICKHOUSE_PORT", "9000"),
		"CLICKHOUSE_DB=" + db,
		"BULK_MAX_AGE_MS=100",
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
	}, extra...)
}

// syncBuffer collects a process's output; the process writes it while the
// test may read it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// runGigapipe runs gigapipe to completion, as with MODE=init_only, and returns
// its output. It fails the test if gigapipe exits with an error or runs
// longer than timeout.
func runGigapipe(t *testing.T, env []string, timeout time.Duration) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, gigapipeBinary(t))
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("gigapipe still running after %v; output:\n%s", timeout, out)
	}
	if err != nil {
		t.Fatalf("gigapipe: %v; output:\n%s", err, out)
	}
	return string(out)
}

// startGigapipe starts gigapipe on a free port and waits until it answers. It
// stops the process when the test ends and prints its output if the test
// failed. It returns the base URL.
//
// The port is found by binding to port 0 and releasing it, so another process
// can take it before gigapipe binds it. When gigapipe exits because the port
// is in use, it is started again on another port; any other early exit fails
// the test.
func startGigapipe(t *testing.T, env []string) string {
	t.Helper()
	for attempt := 1; ; attempt++ {
		base, inUse := tryStartGigapipe(t, env)
		if !inUse {
			return base
		}
		if attempt == 3 {
			t.Fatal("gigapipe could not bind a free port in 3 attempts")
		}
	}
}

// tryStartGigapipe starts gigapipe once. It returns inUse when gigapipe
// exited because its port was taken.
func tryStartGigapipe(t *testing.T, env []string) (base string, inUse bool) {
	t.Helper()
	l, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	var output syncBuffer
	cmd := exec.Command(gigapipeBinary(t))
	cmd.Env = append(env, "PORT="+strconv.Itoa(port))
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	stop := func() {
		_ = cmd.Process.Kill()
		<-exited
	}

	base = fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(60 * time.Second)
	for {
		select {
		case <-exited:
			if strings.Contains(output.String(), "address already in use") {
				return "", true
			}
			t.Fatalf("gigapipe on port %d exited during startup; output:\n%s", port, output.String())
		default:
		}
		resp, err := http.Get(base + "/api/v1/status/buildinfo")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				t.Cleanup(func() {
					stop()
					if t.Failed() {
						t.Logf("gigapipe output:\n%s", output.String())
					}
				})
				return base, false
			}
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatalf("gigapipe on port %d not ready after 60s; output:\n%s", port, output.String())
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// clickhouseQuery runs a query on the ClickHouse HTTP interface and returns
// the body.
func clickhouseQuery(t *testing.T, query string) string {
	t.Helper()
	return clickhouseQueryAt(t, envOr("CLICKHOUSE_HTTP_URL", "http://localhost:8123"), query)
}

// clickhouseQueryAt runs a query on the ClickHouse HTTP interface at endpoint
// and returns the body.
func clickhouseQueryAt(t *testing.T, endpoint, query string) string {
	t.Helper()
	resp, err := http.Post(endpoint+"/?"+url.Values{"query": {query}}.Encode(), "text/plain", nil)
	if err != nil {
		t.Fatalf("ClickHouse %s: %v", endpoint, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ClickHouse query %q: status %d, body %q", query, resp.StatusCode, body)
	}
	return string(body)
}

// freshDatabase returns the name of a database no other test uses, dropped
// when the test ends. gigapipe creates it on startup.
func freshDatabase(t *testing.T, prefix string) string {
	t.Helper()
	db := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	t.Cleanup(func() { clickhouseQuery(t, "DROP DATABASE IF EXISTS "+db) })
	return db
}

// databaseTables lists the tables and views of a database, sorted.
func databaseTables(t *testing.T, db string) []string {
	t.Helper()
	return databaseTablesAt(t, envOr("CLICKHOUSE_HTTP_URL", "http://localhost:8123"), db)
}

// databaseTablesAt lists the tables and views of a database on the ClickHouse
// HTTP interface at endpoint, sorted.
func databaseTablesAt(t *testing.T, endpoint, db string) []string {
	t.Helper()
	out := clickhouseQueryAt(t, endpoint, fmt.Sprintf("SELECT name FROM system.tables WHERE database = '%s' ORDER BY name", db))
	return strings.Fields(out)
}
