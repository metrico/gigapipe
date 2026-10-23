package nestedset

import (
	"strings"
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	got, err := ConfigFromEnv()
	want := Config{Idle: 10 * time.Second, Interval: 5 * time.Second, MaxSpans: 200000}
	if err != nil || got != want {
		t.Errorf("got %+v, %v; want %+v", got, err, want)
	}
	if got.LeaseLen() != 15*time.Second {
		t.Errorf("lease %s, want 3 × the interval", got.LeaseLen())
	}
}

func TestConfigReadsTheSettings(t *testing.T) {
	t.Setenv(IdleEnv, "1s")
	t.Setenv(IntervalEnv, "250ms")
	t.Setenv(MaxSpansEnv, "500")
	got, err := ConfigFromEnv()
	want := Config{Idle: time.Second, Interval: 250 * time.Millisecond, MaxSpans: 500}
	if err != nil || got != want {
		t.Errorf("got %+v, %v; want %+v", got, err, want)
	}
}

func TestConfigRejectsInvalidValues(t *testing.T) {
	for _, c := range []struct{ env, val string }{
		{IdleEnv, "10"},
		{IdleEnv, "0s"},
		{IntervalEnv, "-1s"},
		{IntervalEnv, "soon"},
		{MaxSpansEnv, "0"},
		{MaxSpansEnv, "lots"},
	} {
		t.Run(c.env+"="+c.val, func(t *testing.T) {
			t.Setenv(c.env, c.val)
			if _, err := ConfigFromEnv(); err == nil || !strings.Contains(err.Error(), c.env) {
				t.Errorf("got %v, want an error naming %s", err, c.env)
			}
		})
	}
}
