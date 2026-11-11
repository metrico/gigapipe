package traceql_ir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSampleFraction(t *testing.T) {
	for _, tc := range []struct {
		hints string
		want  float64
		ok    bool
	}{
		{``, 0, false},
		{` with(sample=0.1)`, 0.1, true},
		{` with(exemplars=2, sample=0.25)`, 0.25, true},
		{` with(sample=true)`, 0, false},
		{` with(sample=false)`, 0, false},
		{` with(sample=1)`, 0, false},
		{` with(sample=0)`, 0, false},
		{` with(sample=1.5)`, 0, false},
		{` with(sample=x)`, 0, false},
		{` with(sample=NaN)`, 0, false},
		{` with(sample=Inf)`, 0, false},
		{` with(sample=true, sample=0.5)`, 0.5, true},
		{` with(sample=0.5, sample=0.1)`, 0.5, true},
		{` with(most_recent=true, foo=0.5)`, 0, false},
		{` | topk(2) with(sample=0.5)`, 0.5, true},
	} {
		t.Run(tc.hints, func(t *testing.T) {
			got, ok := mustParse(t, `{} | rate()`+tc.hints).SampleFraction()
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestExemplarLimit(t *testing.T) {
	for _, tc := range []struct {
		hints string
		want  int
		ok    bool
	}{
		{``, 0, false},
		{` with(exemplars=false)`, 0, true},
		{` with(exemplars=5)`, 5, true},
		{` with(exemplars=0)`, 0, true},
		{` with(exemplars=true)`, 0, false},
		{` with(exemplars=1.5)`, 0, false},
		{` with(exemplars=x)`, 0, false},
		{` with(sample=0.1, exemplars=3)`, 3, true},
		{` with(exemplars=x, exemplars=2)`, 2, true},
		{` | topk(2) with(exemplars=false)`, 0, true},
	} {
		t.Run(tc.hints, func(t *testing.T) {
			got, ok := mustParse(t, `{} | rate()`+tc.hints).ExemplarLimit()
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestNeedsWholeTrace(t *testing.T) {
	for query, want := range map[string]bool{
		`{.a = "a"}`:                              false,
		`{.a = "a"} | {.b = "b"}`:                 false,
		`{.a = "a"} | select(.x) | coalesce()`:    false,
		`({.a = "a"})`:                            false,
		`{.a = "a"} && {.b = "b"}`:                true,
		`{.a = "a"} || {.b = "b"}`:                true,
		`{.a = "a"} > {.b = "b"}`:                 true,
		`{.a = "a"} | {.b = "b"} >> {}`:           true,
		`{.a = "a"} | count() > 1`:                true,
		`count() > 1`:                             true,
		`{.a = "a"} | by(.b)`:                     true,
		`{.a = "a"} | ({.b = "b"} | count() > 1)`: true,
		`({.a = "a"} | {.b = "b"}) | select(.x)`:  false,
	} {
		require.Equal(t, want, mustParse(t, query).NeedsWholeTrace(), query)
	}
}
