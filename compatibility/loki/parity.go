// Package loki sends the in-house LogQL queries in queries.txt to a reference
// Loki and to gigapipe, both fed the same logs, and compares the answers. The
// comparison itself is TestLogQLParity (build tag parity), which talks to the
// servers through server.go; this file holds the query list parser and the
// answer normalizer it uses.
package loki

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

// query is one line of queries.txt.
type query struct {
	section string // the documentation section it exercises
	kind    string // "range" or "instant"
	text    string // LogQL, with $RUN standing for the run label value
}

// parseQueries reads queries.txt. Every query must use $RUN, so it only reads
// one run's data, and must make a unique subtest name.
func parseQueries(r io.Reader) ([]query, error) {
	var (
		out     []query
		section string
		names   = map[string]bool{}
	)
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == "" || (strings.HasPrefix(line, "#") && !strings.HasPrefix(line, "## ")):
			continue
		case strings.HasPrefix(line, "## "):
			section = strings.TrimPrefix(line, "## ")
			continue
		}
		kind, text, ok := strings.Cut(line, " ")
		if !ok || (kind != "range" && kind != "instant") {
			return nil, fmt.Errorf("line %d: want \"range <query>\" or \"instant <query>\", got %q", n, line)
		}
		if section == "" {
			return nil, fmt.Errorf("line %d: query before any \"## <section>\" line", n)
		}
		if !strings.Contains(text, "$RUN") {
			return nil, fmt.Errorf("line %d: query doesn't use $RUN: %q", n, text)
		}
		// "/" would split the subtest name into levels.
		if strings.Contains(text, "/") {
			return nil, fmt.Errorf("line %d: query contains \"/\": %q", n, text)
		}
		name := strings.ReplaceAll(text, " ", "_")
		if names[name] {
			return nil, fmt.Errorf("line %d: duplicate query %q", n, text)
		}
		names[name] = true
		out = append(out, query{section: section, kind: kind, text: text})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no queries")
	}
	return out, nil
}

// normalizer replaces what differs between runs with fixed values: the run
// label value becomes "RUN" and timestamps become offsets from the fixture
// start, such as "+20s". Results are sorted by labels, except for sort and
// sort_desc, whose order is their answer.
type normalizer struct {
	run       string
	start     time.Time
	keepOrder bool
}

func (n normalizer) labels(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		if v == n.run {
			v = "RUN"
		}
		out[k] = v
	}
	return out
}

func (n normalizer) offset(t time.Time) string {
	d := t.Sub(n.start)
	if d < 0 {
		return "-" + (-d).String()
	}
	return "+" + d.String()
}

// sampleTime reads a sample timestamp, a JSON number in seconds.
func (n normalizer) sampleTime(v json.Number) (string, error) {
	f, err := v.Float64()
	if err != nil {
		return "", fmt.Errorf("sample time %q: %w", v, err)
	}
	return n.offset(time.UnixMilli(int64(f*1000 + 0.5))), nil
}

// sampleValue reads a sample value, a JSON string holding a number, and
// formats it the same way for both servers.
func sampleValue(v string) string {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return "unparsable: " + v
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func labelKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k + "=" + strconv.Quote(m[k]) + ",")
	}
	return sb.String()
}

func (n normalizer) response(body []byte) (any, error) {
	var resp struct {
		Data struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode response: %w: %.300s", err, body)
	}
	type entry struct {
		key   string
		value any
	}
	var entries []entry
	decode := func(v any) error {
		d := json.NewDecoder(bytes.NewReader(resp.Data.Result))
		d.UseNumber()
		return d.Decode(v)
	}
	switch resp.Data.ResultType {
	case "streams":
		var result []struct {
			Stream map[string]string `json:"stream"`
			Values [][2]string       `json:"values"`
		}
		if err := decode(&result); err != nil {
			return nil, err
		}
		for _, r := range result {
			labels := n.labels(r.Stream)
			var values [][2]string
			for _, v := range r.Values {
				ns, err := strconv.ParseInt(v[0], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("line timestamp %q: %w", v[0], err)
				}
				values = append(values, [2]string{n.offset(time.Unix(0, ns)), v[1]})
			}
			sortTies(values)
			entries = append(entries, entry{labelKey(labels), map[string]any{"stream": labels, "values": values}})
		}
	case "matrix":
		var result []struct {
			Metric map[string]string `json:"metric"`
			Values [][2]any          `json:"values"`
		}
		if err := decode(&result); err != nil {
			return nil, err
		}
		for _, r := range result {
			labels := n.labels(r.Metric)
			var values [][2]string
			for _, v := range r.Values {
				ts, err := n.sampleTime(jsonNumber(v[0]))
				if err != nil {
					return nil, err
				}
				val, _ := v[1].(string)
				values = append(values, [2]string{ts, sampleValue(val)})
			}
			entries = append(entries, entry{labelKey(labels), map[string]any{"metric": labels, "values": values}})
		}
	case "vector":
		var result []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
		}
		if err := decode(&result); err != nil {
			return nil, err
		}
		for _, r := range result {
			labels := n.labels(r.Metric)
			ts, err := n.sampleTime(jsonNumber(r.Value[0]))
			if err != nil {
				return nil, err
			}
			val, _ := r.Value[1].(string)
			entries = append(entries, entry{labelKey(labels), map[string]any{"metric": labels, "value": [2]string{ts, sampleValue(val)}}})
		}
	case "scalar":
		var result [2]any
		if err := decode(&result); err != nil {
			return nil, err
		}
		ts, err := n.sampleTime(jsonNumber(result[0]))
		if err != nil {
			return nil, err
		}
		val, _ := result[1].(string)
		return map[string]any{"resultType": "scalar", "result": [2]string{ts, sampleValue(val)}}, nil
	default:
		return nil, fmt.Errorf("unexpected resultType %q: %.300s", resp.Data.ResultType, body)
	}
	if !n.keepOrder {
		sort.SliceStable(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	}
	result := make([]any, 0, len(entries))
	for _, e := range entries {
		result = append(result, e.value)
	}
	return map[string]any{"resultType": resp.Data.ResultType, "result": result}, nil
}

// sortTies orders the entries of a stream that share a timestamp by their
// line. LogQL defines no order among them, and when several streams merge into
// one (keep, drop, label_format) their lines often share timestamps. The order
// of different timestamps is left as it is.
func sortTies(values [][2]string) {
	for i := 0; i < len(values); {
		j := i + 1
		for j < len(values) && values[j][0] == values[i][0] {
			j++
		}
		tie := values[i:j]
		sort.Slice(tie, func(a, b int) bool { return tie[a][1] < tie[b][1] })
		i = j
	}
}

func jsonNumber(v any) json.Number {
	switch x := v.(type) {
	case json.Number:
		return x
	case string:
		return json.Number(x)
	default:
		return json.Number(fmt.Sprint(x))
	}
}

// canonical encodes a normalized answer; equal answers encode the same.
func canonical(v any) string {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(out)
}
