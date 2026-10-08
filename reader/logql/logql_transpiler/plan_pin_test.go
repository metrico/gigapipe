package logql_transpiler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// pinnedQueries are served by the metrics_15s shortcut on the 15s lattice,
// plus one log query; their SQL and root processor are pinned. Values are
// checked in shortcut_clickhouse_test.go and rawsql_clickhouse_test.go.
var pinnedQueries = []string{
	`rate({job="a"} [5m])`,
	`count_over_time({job="a"} [1h])`,
	`count_over_time({job="a"} |= "" [15m])`,
	`sum by (l) (count_over_time({job="a"} [5m]))`,
	`sum by (l) (rate({job="a"} [15m]))`,
	`count_over_time({job="a"} [5m] offset 7m)`,
	`count_over_time({job="a"} [5m]) > 3`,
	`topk(2, sum by (l) (rate({job="a"} [5m])))`,
	`rate({job="a"} [5m]) / rate({job="b"} [5m])`,
	`{job="a"} |= "x"`,
}

var pinD0 = time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)

// pinnedRequests covers aligned and unaligned starts, step below, at and
// above R, and instant queries on and off a boundary.
var pinnedRequests = map[string]request{
	"range/60s":           rangeReq(pinD0.Add(6*time.Hour), pinD0.Add(18*time.Hour), time.Minute),
	"range/300s":          rangeReq(pinD0.Add(6*time.Hour), pinD0.Add(18*time.Hour), 5*time.Minute),
	"range/300s+60":       rangeReq(pinD0.Add(6*time.Hour+time.Minute), pinD0.Add(18*time.Hour+time.Minute), 5*time.Minute),
	"range/900s+720":      rangeReq(pinD0.Add(6*time.Hour+12*time.Minute), pinD0.Add(18*time.Hour+12*time.Minute), 15*time.Minute),
	"range/3600s":         rangeReq(pinD0.Add(6*time.Hour), pinD0.Add(18*time.Hour), time.Hour),
	"range/7200s+4020":    rangeReq(pinD0.Add(6*time.Hour+4020*time.Second), pinD0.Add(18*time.Hour+4020*time.Second), 2*time.Hour),
	"instant":             instantReq(pinD0.Add(12 * time.Hour)),
	"instant+7m":          instantReq(pinD0.Add(12*time.Hour + 7*time.Minute)),
	"range/300s/nom15s":   {start: pinD0.Add(6 * time.Hour), end: pinD0.Add(18 * time.Hour), step: 5 * time.Minute, noMetrics15s: true},
	"instant+7m/nom15s":   {start: pinD0.Add(12*time.Hour + 7*time.Minute), end: pinD0.Add(12*time.Hour + 7*time.Minute), step: time.Second, instant: true, noMetrics15s: true},
	"range/60s+60/nom15s": {start: pinD0.Add(6*time.Hour + time.Minute), end: pinD0.Add(18*time.Hour + time.Minute), step: time.Minute, noMetrics15s: true},
}

const pinFile = "testdata/sql_plans.json"

type pin struct {
	Root string `json:"root"`
	SQL  string `json:"sql_sha256"`
}

// TestNonGoPlansArePinned pins the SQL and root processor of every query
// outside the Go path; PIN_UPDATE=1 re-records.
func TestNonGoPlansArePinned(t *testing.T) {
	got := map[string]pin{}
	for _, q := range pinnedQueries {
		for name, req := range pinnedRequests {
			p := runRequest(t, q, req, nil)
			sum := sha256.Sum256([]byte(strings.Join(p.sql, "\n;\n")))
			got[q+" @ "+name] = pin{Root: fmt.Sprintf("%T", p.root), SQL: hex.EncodeToString(sum[:])}
		}
	}
	if os.Getenv("PIN_UPDATE") == "1" {
		js, _ := json.MarshalIndent(got, "", " ")
		if err := os.MkdirAll(filepath.Dir(pinFile), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(pinFile, append(js, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	raw, err := os.ReadFile(pinFile)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]pin{}
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(got) != len(want) {
		t.Errorf("%d cases, %d pinned", len(got), len(want))
	}
	for _, k := range keys {
		if got[k] != want[k] {
			t.Errorf("%s: got %+v, pinned %+v", k, got[k], want[k])
		}
	}
}
