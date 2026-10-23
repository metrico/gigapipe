package nestedset

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	// IdleEnv is how long a trace receives no spans before it is numbered.
	IdleEnv = "GIGAPIPE_NESTED_SET_NUMBERER_IDLE"
	// IntervalEnv is the time between passes.
	IntervalEnv = "GIGAPIPE_NESTED_SET_NUMBERER_INTERVAL"
	// MaxSpansEnv caps the spans one pass numbers.
	MaxSpansEnv = "GIGAPIPE_NESTED_SET_NUMBERER_MAX_SPANS"
)

// Config holds the numberer settings.
type Config struct {
	Idle, Interval time.Duration
	MaxSpans       int
}

// LeaseLen is how long a claim or renewal of the lease lasts: 3 × Interval.
func (c Config) LeaseLen() time.Duration { return 3 * c.Interval }

// ConfigFromEnv reads the settings.
func ConfigFromEnv() (Config, error) {
	cfg := Config{Idle: 10 * time.Second, Interval: 5 * time.Second, MaxSpans: 200000}
	for _, d := range []struct {
		env string
		to  *time.Duration
	}{{IdleEnv, &cfg.Idle}, {IntervalEnv, &cfg.Interval}} {
		v := os.Getenv(d.env)
		if v == "" {
			continue
		}
		parsed, err := time.ParseDuration(v)
		if err != nil || parsed <= 0 {
			return cfg, fmt.Errorf("%s=%q: want a positive Go duration such as 10s", d.env, v)
		}
		*d.to = parsed
	}
	if v := os.Getenv(MaxSpansEnv); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return cfg, fmt.Errorf("%s=%q: want a positive integer", MaxSpansEnv, v)
		}
		cfg.MaxSpans = n
	}
	return cfg, nil
}
