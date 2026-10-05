// Package metricretention reads the lifetimes of the metric retention tiers.
package metricretention

import (
	"fmt"
	"strconv"
	"sync"
)

// Tiers holds the lifetime in days of each metric retention tier.
type Tiers struct {
	RawDays        int
	FiveMinuteDays int
	HourDays       int
}

// Settings are the metric retention settings every database shares. A zero day count
// leaves its lifetime at the default, which depends on the database's TTL.
type Settings struct {
	RawDays        int
	FiveMinuteDays int
	HourDays       int
	// RollupDays is the lifetime of the metrics_15s log rollup.
	RollupDays int
	// ReadTier forces every PromQL query onto one tier: raw, 5m or 1h; empty when unset.
	ReadTier string
}

// defaultSamplesDays is the TTL of a database without ttl_days, SAMPLES_DAYS' default.
const defaultSamplesDays = 7

// FromEnv reads the settings through getenv. A day count must be a positive whole number.
func FromEnv(getenv func(string) string) (Settings, error) {
	s := Settings{ReadTier: getenv("METRICS_READ_TIER")}
	for _, d := range []struct {
		name string
		dst  *int
	}{
		{"METRICS_RAW_DAYS", &s.RawDays},
		{"METRICS_5M_DAYS", &s.FiveMinuteDays},
		{"METRICS_1H_DAYS", &s.HourDays},
		{"METRICS_15S_TTL_DAYS", &s.RollupDays},
	} {
		v := getenv(d.name)
		if v == "" {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return Settings{}, fmt.Errorf("%s: invalid value %q, want a positive number of days", d.name, v)
		}
		*d.dst = n
	}
	return s, nil
}

// Tiers resolves the tier lifetimes of a database whose TTL is samplesDays. The raw tier
// defaults to samplesDays; an unset coarser tier defaults to its own default or the finer
// tier's lifetime, whichever is longer.
func (s Settings) Tiers(samplesDays int) (Tiers, error) {
	if samplesDays <= 0 {
		samplesDays = defaultSamplesDays
	}
	t := Tiers{RawDays: orDefault(s.RawDays, samplesDays)}
	t.FiveMinuteDays = orDefault(s.FiveMinuteDays, max(30, t.RawDays))
	t.HourDays = orDefault(s.HourDays, max(365, t.FiveMinuteDays))
	if t.FiveMinuteDays < t.RawDays {
		return Tiers{}, fmt.Errorf("METRICS_5M_DAYS (%d) must not be shorter than METRICS_RAW_DAYS (%d)",
			t.FiveMinuteDays, t.RawDays)
	}
	if t.HourDays < t.FiveMinuteDays {
		return Tiers{}, fmt.Errorf("METRICS_1H_DAYS (%d) must not be shorter than METRICS_5M_DAYS (%d)",
			t.HourDays, t.FiveMinuteDays)
	}
	return t, nil
}

// Rollup is the metrics_15s lifetime of a database whose TTL is samplesDays.
func (s Settings) Rollup(samplesDays int) int {
	return orDefault(s.RollupDays, samplesDays)
}

func orDefault(days, def int) int {
	if days == 0 {
		return def
	}
	return days
}

var (
	mtx        sync.RWMutex
	configured Settings
)

// Configure sets the settings start-up read from the environment.
func Configure(s Settings) {
	mtx.Lock()
	defer mtx.Unlock()
	configured = s
}

// Configured returns the settings set by Configure; zero before it is called.
func Configured() Settings {
	mtx.RLock()
	defer mtx.RUnlock()
	return configured
}
