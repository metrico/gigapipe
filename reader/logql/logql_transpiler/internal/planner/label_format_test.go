package planner

import (
	"context"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_parser"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
)

func TestLabelFormatConstOnNilLabels(t *testing.T) {
	ctx := &shared.PlannerContext{Ctx: context.Background()}
	in := feedEntries(3) // entries have nil Labels
	p := &LabelFormatPlanner{
		GenericPlanner: GenericPlanner{&passthroughProcessor{in}},
		LabelFormat: &logql_parser.LabelFormat{LabelFormatOps: []logql_parser.LabelFormatOp{{
			Label:    logql_parser.LabelName{Name: "foo"},
			ConstVal: &logql_parser.QuotedString{Str: `"bar"`},
		}}},
	}
	out, err := p.Process(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	got := collectEntries(out)
	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3", len(got))
	}
	for i, e := range got {
		if e.Err != nil {
			t.Fatalf("entry %d: unexpected error %v", i, e.Err)
		}
		if e.Labels["foo"] != "bar" {
			t.Errorf("entry %d: foo = %q, want %q", i, e.Labels["foo"], "bar")
		}
	}
}

func TestWrapProcessRecoversPanic(t *testing.T) {
	ctx := &shared.PlannerContext{Ctx: context.Background()}
	in := feedEntries(1)
	g := GenericPlanner{&passthroughProcessor{in}}
	out, err := g.WrapProcess(ctx, in, GenericPlannerOps{
		OnEntry: func(*shared.LogEntry) error { panic("boom") },
	})
	if err != nil {
		t.Fatal(err)
	}
	got := collectEntries(out)
	if len(got) != 1 || got[0].Err == nil {
		t.Fatalf("want one error entry from the recovered panic, got %+v", got)
	}
}
