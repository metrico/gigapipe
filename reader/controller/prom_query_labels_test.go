package controller

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestLabelEndpointTimesParseAsTheSharedParser(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want *int64
	}{
		{"seconds", "1785416892", ms(1785416892000)},
		{"fractional seconds", "1785416892.5", ms(1785416892500)},
		{"milliseconds", "1785416892123", ms(1785416892123)},
		{"microseconds", "1785416892123456", ms(1785416892123)},
		{"nanoseconds", "1785416892123456789", ms(1785416892123)},
		{"rfc3339", "2026-07-30T13:00:00Z", ms(1785416400000)},
		{"rfc3339 fraction", "2026-07-30T13:00:00.25Z", ms(1785416400250)},
		{"prometheus min time", "-292273086-05-16T16:47:06Z", nil},
		{"prometheus max time", "292277025-08-18T07:12:54.999999999Z", nil},
		{"absent", "", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := optionalTimeMs(c.raw, "start")
			if err != nil {
				t.Fatal(err)
			}
			if (got == nil) != (c.want == nil) || got != nil && *got != *c.want {
				t.Fatalf("got %v, want %v", deref(got), deref(c.want))
			}
			if c.want == nil {
				return
			}
			shared, err := ParseTimeSecOrRFC(c.raw, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if shared.UnixMilli() != *got {
				t.Fatalf("got %d, ParseTimeSecOrRFC gives %d", *got, shared.UnixMilli())
			}
		})
	}
}

func TestLabelEndpointTimesRejectGarbage(t *testing.T) {
	for _, raw := range []string{"yesterday", "12:00", "1.2.3"} {
		if _, err := optionalTimeMs(raw, "end"); err == nil {
			t.Errorf("%q: expected an error", raw)
		}
	}
}

func ms(v int64) *int64 { return &v }

func deref(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

func TestLabelEndpointsAnswerInPrometheusEnvelope(t *testing.T) {
	for name, tc := range map[string]struct {
		data      any
		truncated bool
		want      string
	}{
		"label names": {[]string{"__name__", "job"}, false,
			`{"status":"success","data":["__name__","job"]}`},
		"series truncated": {[]map[string]string{{"job": "a<b", "__name__": "up"}}, true,
			`{"status":"success","data":[{"__name__":"up","job":"a\u003cb"}],"warnings":["results truncated due to limit"]}`},
		"empty": {nil, false, `{"status":"success","data":null}`},
	} {
		w := httptest.NewRecorder()
		promRespond(w, tc.data, tc.truncated)
		if w.Code != 200 || w.Header().Get("Content-Type") != "application/json" {
			t.Errorf("%s: status %d, content type %q", name, w.Code, w.Header().Get("Content-Type"))
		}
		if got := w.Body.String(); got != tc.want {
			t.Errorf("%s:\ngot  %s\nwant %s", name, got, tc.want)
		}
	}
}
