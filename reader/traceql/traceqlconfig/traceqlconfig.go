// Package traceqlconfig holds the server settings of TraceQL evaluation.
package traceqlconfig

import (
	"fmt"
	"os"
	"strconv"
)

const (
	// AncestryRoundsEnv sets the ancestry rounds R of structural operators:
	// traces up to 2^R spans deep resolve.
	AncestryRoundsEnv = "GIGAPIPE_TRACEQL_ANCESTRY_ROUNDS"
	// DefaultAncestryRounds applies when AncestryRoundsEnv is unset.
	DefaultAncestryRounds = 20
	maxAncestryRounds     = 32

	// PortionSpansEnv sets the most spans a TraceQL search or tag autocomplete
	// reads per portion.
	PortionSpansEnv = "GIGAPIPE_TRACEQL_PORTION_SPANS"
	// DefaultPortionSpans applies when PortionSpansEnv is unset.
	DefaultPortionSpans = 2500000
)

var (
	ancestryRounds       = DefaultAncestryRounds
	portionSpans   int64 = DefaultPortionSpans
)

// Init reads the settings from the environment. Call it at startup.
func Init() error {
	if err := initAncestryRounds(); err != nil {
		return err
	}
	return initPortionSpans()
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

// AncestryRounds returns the configured pointer-jumping rounds.
func AncestryRounds() int {
	return ancestryRounds
}

// PortionSpans returns the span budget of one portion.
func PortionSpans() int64 {
	return portionSpans
}
