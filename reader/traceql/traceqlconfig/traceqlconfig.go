// Package traceqlconfig holds the server settings of TraceQL evaluation.
package traceqlconfig

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	// AncestryRoundsEnv sets the ancestry rounds R of structural operators:
	// traces up to 2^R spans deep resolve.
	AncestryRoundsEnv = "GIGAPIPE_TRACEQL_ANCESTRY_ROUNDS"
	// DefaultAncestryRounds applies when AncestryRoundsEnv is unset.
	DefaultAncestryRounds = 20
	maxAncestryRounds     = 32

	// PortionSpansEnv sets the most spans a TraceQL tag autocomplete reads per
	// portion on one shard.
	PortionSpansEnv = "GIGAPIPE_TRACEQL_PORTION_SPANS"
	// DefaultPortionSpans applies when PortionSpansEnv is unset.
	DefaultPortionSpans = 7500000

	// SearchMaxDurationEnv sets the longest range of a TraceQL search or tag
	// autocomplete; 0 means no limit.
	SearchMaxDurationEnv = "GIGAPIPE_TRACEQL_SEARCH_MAX_DURATION"
	// DefaultSearchMaxDuration applies when SearchMaxDurationEnv is unset.
	DefaultSearchMaxDuration = 168 * time.Hour
)

var (
	ancestryRounds          = DefaultAncestryRounds
	portionSpans      int64 = DefaultPortionSpans
	searchMaxDuration       = DefaultSearchMaxDuration
)

// Init reads the settings from the environment. Call it at startup.
func Init() error {
	if err := initAncestryRounds(); err != nil {
		return err
	}
	if err := initPortionSpans(); err != nil {
		return err
	}
	return initSearchMaxDuration()
}

func initAncestryRounds() error {
	ancestryRounds = DefaultAncestryRounds
	v := os.Getenv(AncestryRoundsEnv)
	if v == "" {
		return nil
	}
	rounds, err := strconv.Atoi(v)
	if err != nil || rounds < 1 || rounds > maxAncestryRounds {
		return fmt.Errorf("%s=%q: want an integer from 1 to %d", AncestryRoundsEnv, v, maxAncestryRounds)
	}
	ancestryRounds = rounds
	return nil
}

func initPortionSpans() error {
	portionSpans = DefaultPortionSpans
	v := os.Getenv(PortionSpansEnv)
	if v == "" {
		return nil
	}
	spans, err := strconv.ParseInt(v, 10, 64)
	if err != nil || spans < 1 {
		return fmt.Errorf("%s=%q: want a positive integer", PortionSpansEnv, v)
	}
	portionSpans = spans
	return nil
}

func initSearchMaxDuration() error {
	searchMaxDuration = DefaultSearchMaxDuration
	v := os.Getenv(SearchMaxDurationEnv)
	if v == "" {
		return nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return fmt.Errorf("%s=%q: want a non-negative duration such as 168h, or 0 for no limit", SearchMaxDurationEnv, v)
	}
	searchMaxDuration = d
	return nil
}

// AncestryRounds returns the configured pointer-jumping rounds.
func AncestryRounds() int {
	return ancestryRounds
}

// PortionSpans returns the span budget of one portion on one shard.
func PortionSpans() int64 {
	return portionSpans
}

// SearchMaxDuration returns the longest search range, 0 meaning no limit.
func SearchMaxDuration() time.Duration {
	return searchMaxDuration
}
