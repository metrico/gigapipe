package controller

import (
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
