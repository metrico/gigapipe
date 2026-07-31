package main

import (
	"slices"
	"strings"
	"testing"
)

// stepNames renders the boot sequence for a mode as an ordered slice of names.
func stepNames(mode string) []string {
	seq := bootSequence(mode)
	names := make([]string, len(seq))
	for i, s := range seq {
		names[i] = s.name
	}
	return names
}

// orderViolations reports every init-ordering invariant start() depends on that
// the given sequence breaks. Empty result means the order is safe.
//
// Two independent constraints pull in opposite directions, so both must hold at
// once:
//
//   - reader BEFORE ruler: reader.Init populates the reader registry the ruler
//     binds its rule sessions to; ordering reader after ruler yields a nil
//     session (the regression that shipped on alpha via #864).
//   - view LAST: view registers a wildcard "/" catch-all route that shadows any
//     route registered after it; ordering view before ruler hides the ruler's
//     HTTP routes (the regression that lived on master).
//
// The safe order writer -> reader -> ruler -> view is the only one that
// satisfies both: reader ahead of ruler, view still dead last.
func orderViolations(names []string) []string {
	writer := slices.Index(names, "writer")
	reader := slices.Index(names, "reader")
	ruler := slices.Index(names, "ruler")
	view := slices.Index(names, "view")

	// Constraints below only apply to subsystems actually present in the mode.
	var v []string
	// The ruler's write-back uses the writer's ClickHouse client.
	if writer != -1 && ruler != -1 && writer > ruler {
		v = append(v, "writer must init before ruler")
	}
	// reader.Init populates the reader registry the ruler binds rule sessions to.
	if reader != -1 && ruler != -1 && reader > ruler {
		v = append(v, "reader must init before ruler (else the ruler binds a nil session)")
	}
	// view's wildcard "/" route must be registered after everything else.
	if view != -1 && view != len(names)-1 {
		v = append(v, "view must init last (its catch-all route shadows anything after it)")
	}
	return v
}

// TestBootSequenceOrder pins the subsystem init ordering start() depends on: the
// real sequence for every combined mode must break none of the invariants.
func TestBootSequenceOrder(t *testing.T) {
	for _, mode := range []string{"all", ""} {
		t.Run("mode="+mode, func(t *testing.T) {
			names := stepNames(mode)
			if len(names) != 4 {
				t.Fatalf("mode %q expected 4 subsystems, got %v", mode, names)
			}
			if got := orderViolations(names); len(got) != 0 {
				t.Errorf("mode %q order %v violates: %v", mode, names, got)
			}
		})
	}
}

// TestBootSequenceOrderCatchesKnownRegressions guards the guard: it feeds the two
// historical bad orderings through the same invariant check and asserts each is
// still detected, so neither regression can silently return.
func TestBootSequenceOrderCatchesKnownRegressions(t *testing.T) {
	cases := []struct {
		name  string
		order []string
		want  string // substring the matching violation must contain
	}{
		{
			name:  "reader-after-ruler (alpha #864 nil session)",
			order: []string{"writer", "ruler", "reader", "view"},
			want:  "reader must init before ruler",
		},
		{
			name:  "view-before-ruler (master wildcard shadows routes)",
			order: []string{"writer", "reader", "view", "ruler"},
			want:  "view must init last",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := orderViolations(c.order)
			if len(got) == 0 {
				t.Fatalf("order %v should have been rejected, got no violations", c.order)
			}
			if !slices.ContainsFunc(got, func(s string) bool { return strings.Contains(s, c.want) }) {
				t.Errorf("order %v: expected a violation containing %q, got %v", c.order, c.want, got)
			}
		})
	}
}

// TestBootSequenceModesScope guards the per-mode gating: the ruler only runs in
// the combined modes, and reader-only / writer-only modes stay minimal.
func TestBootSequenceModesScope(t *testing.T) {
	if got := stepNames("reader"); !slices.Equal(got, []string{"reader", "view"}) {
		t.Errorf(`mode "reader" should be [reader view], got %v`, got)
	}
	if got := stepNames("writer"); !slices.Equal(got, []string{"writer"}) {
		t.Errorf(`mode "writer" should be [writer], got %v`, got)
	}
	if slices.Contains(stepNames("reader"), "ruler") {
		t.Error(`mode "reader" must not start the ruler`)
	}
	if slices.Contains(stepNames("writer"), "ruler") {
		t.Error(`mode "writer" must not start the ruler`)
	}
}
