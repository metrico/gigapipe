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
)

var ancestryRounds = DefaultAncestryRounds

// Init reads the settings from the environment. Call it at startup.
func Init() error {
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

// AncestryRounds returns the configured pointer-jumping rounds.
func AncestryRounds() int {
	return ancestryRounds
}
