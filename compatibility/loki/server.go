//go:build parity

package loki

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/metrico/qryn/v5/test/integration/seed"
)

// This file talks to the two servers. Only TestLogQLParity uses it, so it
// carries the same build tag; parity.go holds the parts the unit tests cover.

// server is one LogQL endpoint under comparison.
type server struct {
	name string
	base string
}

var client = &http.Client{Timeout: 30 * time.Second}

func (s server) get(path string, params url.Values) (int, []byte, error) {
	resp, err := client.Get(strings.TrimSuffix(s.base, "/") + path + "?" + params.Encode())
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, body, err
}

func (s server) push(body []byte) error {
	resp, err := client.Post(strings.TrimSuffix(s.base, "/")+"/loki/api/v1/push", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s push: status %d: %s", s.name, resp.StatusCode, msg)
	}
	return nil
}

// window is the time range every query covers.
type window struct {
	start    time.Time // the fixture start; timestamps are reported relative to it
	from, to time.Time // query_range bounds
	at       time.Time // instant query time
}

// ask sends q to s and returns the answer, normalized so the two servers'
// answers can be compared, and for an error answer its body, for display.
func (s server) ask(q query, run string, w window) (any, string, error) {
	text := strings.ReplaceAll(q.text, "$RUN", run)
	var (
		status int
		body   []byte
		err    error
	)
	if q.kind == "range" {
		status, body, err = s.get("/loki/api/v1/query_range", url.Values{
			"query":     {text},
			"start":     {strconv.FormatInt(w.from.UnixNano(), 10)},
			"end":       {strconv.FormatInt(w.to.UnixNano(), 10)},
			"step":      {"30s"},
			"limit":     {"1000"},
			"direction": {"backward"},
		})
	} else {
		status, body, err = s.get("/loki/api/v1/query", url.Values{
			"query": {text},
			"time":  {strconv.FormatInt(w.at.UnixNano(), 10)},
			"limit": {"1000"},
		})
	}
	if err != nil {
		return nil, "", err
	}
	if status != http.StatusOK {
		// Error messages differ between the two; only the status is compared.
		return map[string]any{"httpStatus": status}, strings.TrimSpace(string(body)), nil
	}
	n := normalizer{run: run, start: w.start, keepOrder: strings.HasPrefix(q.text, "sort")}
	v, err := n.response(body)
	return v, "", err
}

// countLines returns how many of the run's log lines s returns.
func (s server) countLines(run string, w window) (int, error) {
	status, body, err := s.get("/loki/api/v1/query_range", url.Values{
		"query":     {fmt.Sprintf("{%s=%q}", seed.RunLabel, run)},
		"start":     {strconv.FormatInt(w.from.UnixNano(), 10)},
		"end":       {strconv.FormatInt(w.to.UnixNano(), 10)},
		"limit":     {"1000"},
		"direction": {"backward"},
	})
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK {
		return 0, fmt.Errorf("status %d: %.300s", status, body)
	}
	var resp struct {
		Data struct {
			Result []struct {
				Values [][2]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, err
	}
	total := 0
	for _, r := range resp.Data.Result {
		total += len(r.Values)
	}
	return total, nil
}

// shorten keeps failure messages readable; it is only applied for display,
// after the full answers were compared.
func shorten(s string) string {
	const max = 4000
	if len(s) > max {
		return s[:max] + "\n... (truncated)"
	}
	return s
}
