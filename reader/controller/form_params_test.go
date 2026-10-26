package controller

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func postForm(target string, form url.Values) *http.Request {
	r := httptest.NewRequest("POST", target, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

// Prometheus clients send keys we do not use (timeout, limit, stats); they
// must not turn a POST query into a 400.
func TestFormParamsIgnoreUnknownKeys(t *testing.T) {
	form := url.Values{
		"query":   {"up"},
		"time":    {"1700000000"},
		"start":   {"1700000000"},
		"end":     {"1700000600"},
		"step":    {"15"},
		"match[]": {"up"},
		"timeout": {"30s"},
	}

	inst, err := parseQueryInstantProps(postForm("/api/v1/query", form))
	if err != nil || inst.Query != "up" || inst.Time.Unix() != 1700000000 {
		t.Fatalf("instant: %+v, %v", inst, err)
	}

	rng, err := parseQueryRangePropsV2(postForm("/api/v1/query_range", form))
	if err != nil || rng.Query != "up" || rng.Start.Unix() != 1700000000 ||
		rng.End.Unix() != 1700000600 || rng.Raw.Step != "15" {
		t.Fatalf("range: %+v, %v", rng, err)
	}

	series, err := ParseLogSeriesParamsV2(postForm("/loki/api/v1/series", form), 1_000_000_000)
	if err != nil || len(series.Match) != 1 || series.Start.Unix() != 1700000000 {
		t.Fatalf("loki series: %+v, %v", series, err)
	}

	labels, err := getLabelsParams(postForm("/api/v1/labels", form))
	if err != nil || len(labels.match) != 1 || labels.end.Unix() != 1700000600 {
		t.Fatalf("prom labels: %+v, %v", labels, err)
	}
}

func TestFormParamsBodyOverridesURL(t *testing.T) {
	r := postForm("/api/v1/query?query=down&time=1", url.Values{"query": {"up"}})
	inst, err := parseQueryInstantProps(r)
	if err != nil || inst.Query != "up" || inst.Time.Unix() != 1 {
		t.Fatalf("%+v, %v", inst, err)
	}
}
