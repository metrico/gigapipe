//go:build integration

// Regression test for the unwrap conversion functions (issue #990): the same
// values must give the same numbers on both LogQL paths. `| regexp` extracts
// the label in ClickHouse and converts it in SQL; `| logfmt` extracts it in Go
// and converts it with the Go parser. A value that one path converts and the
// other drops, or converts to a different number, silently changes
// sum_over_time / avg_over_time results.
//
// Each value is pushed as its own stream, so the per-stream result shows
// exactly which value differs. Run via test/integration/run.sh or against a
// running stack:
//
//	GIGAPIPE_URL=http://localhost:3100 go test -tags integration ./test/integration/... -v

package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"
)

var unwrapConvValues = map[string][]string{
	"duration": {
		"5s", "1m", "0", "3d4h", "garbage", "5m30", "10minutes", "5.s",
		"300μs", "300µs", "300us", "5m30s", "1h", "500ms", "2s", "1m30s",
		"-5s", "-1h30m", "bad", "5", "1d", "abc",
	},
	"bytes": {
		"1.2.3", "1 2", "1024", "1.5kb", "1.5KB", "2kib", "1mb", "1mib",
		"1gb", "1gib", "1tb", "1tib", "1pb", "1pib", "1eb", "1eib",
		"1k", "1K", "1m", "1g", "1t", "1p", "1e", "1,024 KB",
	},
}

// queryVectorByLabel runs an instant metric query and returns the sample
// values keyed by the given label. A stream whose value was dropped is absent.
func queryVectorByLabel(t *testing.T, query, labelKey string) map[string]float64 {
	t.Helper()
	u := fmt.Sprintf("%s/loki/api/v1/query?query=%s", baseURL(), url.QueryEscape(query))
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query %s: status = %d, body = %q", query, resp.StatusCode, raw)
	}

	var out struct {
		Data struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Metric map[string]string `json:"metric"`
				Value  [2]any            `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode query response: %v\nbody: %s", err, raw)
	}
	if out.Data.ResultType != "vector" {
		t.Fatalf("query %s: resultType = %q, want vector\nbody: %s", query, out.Data.ResultType, raw)
	}

	values := map[string]float64{}
	for _, r := range out.Data.Result {
		s, _ := r.Value[1].(string)
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Fatalf("query %s: sample value %q: %v", query, s, err)
		}
		values[r.Metric[labelKey]] = f
	}
	return values
}

// TestLogQLUnwrapConvSQLMatchesGo runs every value through unwrap duration()
// and bytes() on the SQL path and the Go path and fails on any difference.
func TestLogQLUnwrapConvSQLMatchesGo(t *testing.T) {
	waitReady(t)

	run := "itest_unwrap_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	nowNs := strconv.FormatInt(time.Now().UnixNano(), 10)

	var streams []lokiPushStream
	total := 0
	for fn, values := range unwrapConvValues {
		for i, v := range values {
			streams = append(streams, lokiPushStream{
				Stream: map[string]string{"itest_unwrap": run, "fn": fn, "case": strconv.Itoa(i)},
				Values: [][]string{{nowNs, fmt.Sprintf(`v="%s"`, v)}},
			})
			total++
		}
	}
	pushStreams(t, streams)

	// Ingestion is asynchronous (bulk flush); poll until every stream is
	// visible before comparing the two paths.
	count := fmt.Sprintf(`sum(count_over_time({itest_unwrap="%s"}[5m]))`, run)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if got := queryVectorByLabel(t, count, "")[""]; int(got) == total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("streams never became queryable (want %d lines)", total)
		}
		time.Sleep(time.Second)
	}

	// kept returns the converted value per case. A zero sample is left out of
	// the sum result, so count by (case) tells which values were kept at all.
	kept := func(t *testing.T, selector, parser, fn string) map[string]float64 {
		inner := fmt.Sprintf("sum_over_time(%s %s | unwrap %s(v) [5m])", selector, parser, fn)
		sums := queryVectorByLabel(t, "sum by (case) ("+inner+")", "case")
		out := map[string]float64{}
		for c := range queryVectorByLabel(t, "count by (case) ("+inner+")", "case") {
			out[c] = sums[c]
		}
		return out
	}

	for fn, values := range unwrapConvValues {
		t.Run(fn, func(t *testing.T) {
			selector := fmt.Sprintf(`{itest_unwrap="%s", fn="%s"}`, run, fn)
			sqlPath := kept(t, selector, "| regexp `v=\"(?P<v>[^\"]*)\"`", fn)
			goPath := kept(t, selector, "| logfmt", fn)

			for i, v := range values {
				c := strconv.Itoa(i)
				s, sOK := sqlPath[c]
				g, gOK := goPath[c]
				switch {
				case sOK != gOK:
					t.Errorf("%s(%q): regexp (SQL) kept=%v %v, logfmt (Go) kept=%v %v", fn, v, sOK, s, gOK, g)
				case sOK && math.Abs(s-g) > 1e-9*math.Max(1, math.Abs(g)):
					t.Errorf("%s(%q): regexp (SQL) = %v, logfmt (Go) = %v", fn, v, s, g)
				}
			}
		})
	}
}
