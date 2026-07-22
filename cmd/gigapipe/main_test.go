package main

import (
	"slices"
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

// TestBootSequenceOrder pins the subsystem init ordering start() depends on.
//
// It is a regression detector, not a description of the current code: reorder
// reader after ruler (the exact change that shipped on the alpha branch) and
// this test fails, because the ruler then initializes before the reader
// registry it evaluates rules through exists -- and refuses to start / runs
// against a nil session. view moving off the end fails it too, since view's
// catch-all "/" route would shadow anything registered after it.
func TestBootSequenceOrder(t *testing.T) {
	for _, mode := range []string{"all", ""} {
		t.Run("mode="+mode, func(t *testing.T) {
			names := stepNames(mode)

			writer := slices.Index(names, "writer")
			reader := slices.Index(names, "reader")
			ruler := slices.Index(names, "ruler")
			view := slices.Index(names, "view")

			if writer == -1 || reader == -1 || ruler == -1 || view == -1 {
				t.Fatalf("mode %q missing a subsystem, got %v", mode, names)
			}

			// The ruler's write-back uses the writer's ClickHouse client.
			if writer > ruler {
				t.Errorf("writer must init before ruler, got %v", names)
			}
			// reader.Init populates the reader registry the ruler binds its rule
			// sessions to; ordering it after the ruler yields a nil session.
			if reader > ruler {
				t.Errorf("reader must init before ruler so the ruler binds a live "+
					"reader session, got %v", names)
			}
			// view registers a wildcard "/" route and must be last.
			if view != len(names)-1 {
				t.Errorf("view must init last (it adds a catch-all route), got %v", names)
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
