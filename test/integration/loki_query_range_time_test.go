//go:build integration

// The Loki query_range time parameters must resolve the way Loki documents
// them: end defaults to now, start to end minus since (one hour without
// since), an explicit start wins over since, and a window whose end is before
// its start is rejected. index/stats rejects that window too.
//
// Run via test/integration/run.sh, or against a running stack:
//
//	GIGAPIPE_URL=http://localhost:3100 go test -tags integration ./test/integration/... -v

package integration

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"testing"
	"time"
)

// queryRangeLines runs /loki/api/v1/query_range with params and returns the
// status code, the returned log lines (sorted) and the raw body.
func queryRangeLines(t *testing.T, params url.Values) (int, []string, string) {
	t.Helper()
	return getLokiLines(t, "/loki/api/v1/query_range", params)
}

func getLokiLines(t *testing.T, path string, params url.Values) (int, []string, string) {
	t.Helper()
	resp, err := http.Get(baseURL() + path + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, nil, string(raw)
	}
	var out struct {
		Data struct {
			Result []struct {
				Values [][]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %s response: %v\nbody: %s", path, err, raw)
	}
	var lines []string
	for _, s := range out.Data.Result {
		for _, v := range s.Values {
			lines = append(lines, v[1])
		}
	}
	slices.Sort(lines)
	return resp.StatusCode, lines, string(raw)
}

func TestLokiQueryRangeTimeParams(t *testing.T) {
	waitReady(t)

	// Unique label value per run so repeated runs against a shared ClickHouse
	// don't read each other's data.
	stream := "itest_time_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	now := time.Now()
	ago := func(d time.Duration) string { return strconv.FormatInt(now.Add(-d).UnixNano(), 10) }

	// One line inside the last 15 minutes, one inside the last hour, one
	// three hours back.
	pushStreams(t, []lokiPushStream{{
		Stream: map[string]string{"itest_stream": stream},
		Values: [][]string{
			{ago(180 * time.Minute), "old"},
			{ago(40 * time.Minute), "mid"},
			{ago(10 * time.Minute), "new"},
		},
	}})
	query := `{itest_stream="` + stream + `"}`

	// Ingestion is asynchronous (bulk flush); poll with an explicit window
	// until all three lines are visible.
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, lines, _ := queryRangeLines(t, url.Values{"query": {query}, "start": {ago(4 * time.Hour)}, "end": {ago(0)}})
		if len(lines) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lines never became queryable; got %v", lines)
		}
		time.Sleep(time.Second)
	}

	for _, tc := range []struct {
		name   string
		params url.Values
		want   []string
	}{
		{"explicit start and end", url.Values{"start": {ago(50 * time.Minute)}, "end": {ago(20 * time.Minute)}}, []string{"mid"}},
		{"no start or end: the last hour", url.Values{}, []string{"mid", "new"}},
		{"only end: start is one hour before it", url.Values{"end": {ago(150 * time.Minute)}}, []string{"old"}},
		{"since", url.Values{"since": {"15m"}}, []string{"new"}},
		{"since with end", url.Values{"since": {"1h"}, "end": {ago(150 * time.Minute)}}, []string{"old"}},
		{"since longer than an hour", url.Values{"since": {"4h"}}, []string{"mid", "new", "old"}},
		{"start wins over since", url.Values{"since": {"15m"}, "start": {ago(4 * time.Hour)}}, []string{"mid", "new", "old"}},
		{"start equal to end", url.Values{"start": {ago(20 * time.Minute)}, "end": {ago(20 * time.Minute)}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.params.Set("query", query)
			code, lines, raw := queryRangeLines(t, tc.params)
			if code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body: %s", code, raw)
			}
			if !slices.Equal(lines, tc.want) {
				t.Errorf("lines = %v, want %v", lines, tc.want)
			}
		})
	}

	for _, tc := range []struct {
		name, path string
		params     url.Values
	}{
		{"query_range: end before start", "/loki/api/v1/query_range", url.Values{"start": {ago(5 * time.Minute)}, "end": {ago(50 * time.Minute)}}},
		{"query_range: end one nanosecond before start", "/loki/api/v1/query_range", url.Values{
			"start": {ago(20 * time.Minute)}, "end": {strconv.FormatInt(now.Add(-20*time.Minute).UnixNano()-1, 10)}}},
		{"query_range: invalid since", "/loki/api/v1/query_range", url.Values{"since": {"abc"}}},
		{"index/stats: end before start", "/loki/api/v1/index/stats", url.Values{"start": {ago(5 * time.Minute)}, "end": {ago(50 * time.Minute)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.params.Set("query", query)
			code, _, raw := getLokiLines(t, tc.path, tc.params)
			if code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400; body: %s", code, raw)
			}
		})
	}
}
