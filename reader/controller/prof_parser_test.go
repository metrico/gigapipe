package controller

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/prof"
	v1 "github.com/metrico/qryn/v5/reader/prof/types/v1"
	"google.golang.org/protobuf/proto"
)

func parseJSONRequest(body string, res proto.Message) error {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return defaultParser(req, res)
}

// TestDefaultParserJSON pins the request spellings the proto3 JSON mapping
// requires a parser to accept, and the leniency existing clients rely on.
func TestDefaultParserJSON(t *testing.T) {
	const typeID = "process_cpu:cpu:nanoseconds:cpu:nanoseconds"
	const selector = `{service_name="api"}`
	want := &prof.SelectMergeStacktracesRequest{
		ProfileTypeID: typeID,
		LabelSelector: selector,
		Start:         1704067200000,
		End:           1704153600000,
	}

	for name, body := range map[string]string{
		"camelCase, as docs/profiling-api.md documents": `{"profileTypeID":"` + typeID + `","labelSelector":"{service_name=\"api\"}","start":1704067200000,"end":1704153600000}`,
		"proto field names":                             `{"profile_typeID":"` + typeID + `","label_selector":"{service_name=\"api\"}","start":1704067200000,"end":1704153600000}`,
		"int64 as strings":                              `{"profileTypeID":"` + typeID + `","labelSelector":"{service_name=\"api\"}","start":"1704067200000","end":"1704153600000"}`,
		"unknown fields are ignored":                    `{"profileTypeID":"` + typeID + `","labelSelector":"{service_name=\"api\"}","start":1704067200000,"end":1704153600000,"profileIdSelector":["x"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			got := &prof.SelectMergeStacktracesRequest{}
			if err := parseJSONRequest(body, got); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

func TestDefaultParserJSONAggregation(t *testing.T) {
	want := v1.TimeSeriesAggregationType_TIME_SERIES_AGGREGATION_TYPE_AVERAGE
	for _, value := range []string{`1`, `"TIME_SERIES_AGGREGATION_TYPE_AVERAGE"`} {
		got := &prof.SelectSeriesRequest{}
		if err := parseJSONRequest(`{"aggregation":`+value+`}`, got); err != nil {
			t.Errorf("aggregation %s: %v", value, err)
			continue
		}
		if got.Aggregation == nil || *got.Aggregation != want {
			t.Errorf("aggregation %s: got %v, want %v", value, got.Aggregation, want)
		}
	}
}

// TestDefaultParserJSONDrilldown parses the SelectMergeProfile body Grafana
// Profiles Drilldown sends, which mixes spellings and carries a field Gigapipe
// does not know.
func TestDefaultParserJSONDrilldown(t *testing.T) {
	got := &prof.SelectMergeProfileRequest{}
	err := parseJSONRequest(`{"profile_typeID":"t","label_selector":"{}","start":1,"end":2,
		"stackTraceSelector":{"call_site":[{"name":"main"}]},"maxNodes":100,
		"profileIdSelector":["x"]}`, got)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProfileTypeID != "t" || got.LabelSelector != "{}" || got.Start != 1 || got.End != 2 {
		t.Errorf("got %v", got)
	}
}

func TestDefaultParserJSONRejectsInvalidBody(t *testing.T) {
	for _, body := range []string{"", "{", `{"start":"yesterday"}`} {
		if err := parseJSONRequest(body, &prof.SelectMergeStacktracesRequest{}); err == nil {
			t.Errorf("body %q was accepted", body)
		}
	}
}
