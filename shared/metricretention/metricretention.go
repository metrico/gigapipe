// Package metricretention reads the lifetimes of the metric retention tiers.
package metricretention

import (
	"fmt"
	"strconv"
)

// Tiers holds the lifetime in days of each metric retention tier.
type Tiers struct {
	RawDays        int
	FiveMinuteDays int
	HourDays       int
}

// FromEnv reads the tier lifetimes through getenv. The raw tier defaults to
// samplesDays; an unset coarser tier defaults to its own default or the finer
// tier's lifetime, whichever is longer.
func FromEnv(samplesDays int, getenv func(string) string) (Tiers, error) {
	var t Tiers
	var err error
	if t.RawDays, err = days(getenv, "METRICS_RAW_DAYS", samplesDays); err != nil {
		return Tiers{}, err
	}
	if t.FiveMinuteDays, err = days(getenv, "METRICS_5M_DAYS", max(30, t.RawDays)); err != nil {
		return Tiers{}, err
	}
	if t.HourDays, err = days(getenv, "METRICS_1H_DAYS", max(365, t.FiveMinuteDays)); err != nil {
		return Tiers{}, err
	}
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

func days(getenv func(string) string, name string, def int) (int, error) {
	v := getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s: invalid value %q, want a positive number of days", name, v)
	}
	return n, nil
}
