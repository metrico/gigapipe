package ruler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

type fakeEvaluator struct {
	mu    sync.Mutex
	exprs []string
	vec   promql.Vector
	err   error
}

func (f *fakeEvaluator) Evaluate(ctx context.Context, expr string, t time.Time) (promql.Vector, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.exprs = append(f.exprs, expr)
	return f.vec, f.err
}

type writeCall struct {
	record string
	labels map[string]string
	vec    promql.Vector
}

type fakeWriter struct {
	mu     sync.Mutex
	writes []writeCall
}

func (f *fakeWriter) Write(record string, ruleLabels map[string]string, v promql.Vector) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, writeCall{record, ruleLabels, v})
	return nil
}

type fakeReader struct {
	groups NamespaceRuleGroups
}

func (f *fakeReader) GetRuleGroup(ctx context.Context, namespace, groupName string) (RuleGroup, error) {
	return RuleGroup{}, errors.New("not used")
}
func (f *fakeReader) ListRuleGroups(ctx context.Context, namespace string) ([]RuleGroup, error) {
	return nil, errors.New("not used")
}
func (f *fakeReader) GetAllRuleGroups(ctx context.Context) (NamespaceRuleGroups, error) {
	return f.groups, nil
}

func sampleVec() promql.Vector {
	return promql.Vector{{T: 1, F: 2, Metric: labels.FromStrings("__name__", "src")}}
}

func TestEvaluateInterval_EvaluatesMatchingRecordingRuleAndWritesBack(t *testing.T) {
	eval := &fakeEvaluator{vec: sampleVec()}
	writer := &fakeWriter{}
	reader := &fakeReader{groups: NamespaceRuleGroups{
		"ns": {{
			Name:     "g",
			Interval: "30s",
			Rules:    []Rule{{Record: "rec", Expr: "up", Labels: map[string]string{"k": "v"}}},
		}},
	}}
	m := NewRuleManager(eval, reader, writer, time.Minute)
	m.ctx = context.Background()

	m.evaluateInterval(context.Background(), 30*time.Second)

	if len(eval.exprs) != 1 || eval.exprs[0] != "up" {
		t.Fatalf("evaluator exprs = %v, want [up]", eval.exprs)
	}
	if len(writer.writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(writer.writes))
	}
	w := writer.writes[0]
	if w.record != "rec" || w.labels["k"] != "v" {
		t.Errorf("write record/labels mismatch: %+v", w)
	}
}

func TestEvaluateInterval_SkipsNonMatchingIntervalAndAlertingRules(t *testing.T) {
	eval := &fakeEvaluator{vec: sampleVec()}
	writer := &fakeWriter{}
	reader := &fakeReader{groups: NamespaceRuleGroups{
		"ns": {
			{Name: "fast", Interval: "15s", Rules: []Rule{{Record: "rec", Expr: "up"}}},
			{Name: "alerts", Interval: "30s", Rules: []Rule{{Alert: "Down", Expr: "up == 0"}}},
		},
	}}
	m := NewRuleManager(eval, reader, writer, time.Minute)
	m.ctx = context.Background()

	m.evaluateInterval(context.Background(), 30*time.Second)

	if len(eval.exprs) != 0 {
		t.Errorf("nothing should evaluate: interval mismatch + alerting rule, got %v", eval.exprs)
	}
	if len(writer.writes) != 0 {
		t.Errorf("alerting rules must never be written back, got %v", writer.writes)
	}
}

func TestEvaluateRecordingRule_ErrorRecordsHealthAndSkipsWrite(t *testing.T) {
	eval := &fakeEvaluator{err: errors.New("boom")}
	writer := &fakeWriter{}
	reader := &fakeReader{}
	m := NewRuleManager(eval, reader, writer, time.Minute)
	m.ctx = context.Background()

	rule := Rule{Record: "rec", Expr: "up"}
	m.evaluateRecordingRule("ns", "g", rule, time.Now())

	if len(writer.writes) != 0 {
		t.Errorf("failed evaluation must not write, got %v", writer.writes)
	}
	h, ok := m.getRuleHealth("ns", "g", "rec")
	if !ok || h.Health != "err" || h.LastError != "boom" {
		t.Errorf("health not recorded as err: %+v ok=%v", h, ok)
	}
}

func TestPruneHealth_EvictsRemovedRulesKeepsLive(t *testing.T) {
	m := NewRuleManager(nil, nil, nil, time.Minute)
	m.setRuleHealth("ns", "g", "live", RuleHealth{Health: "ok"})
	m.setRuleHealth("ns", "g", "stale", RuleHealth{Health: "ok"})
	m.setRuleHealth("ns", "gone", "x", RuleHealth{Health: "ok"})

	// Only ns/g/live still exists in the rule set.
	m.pruneHealth(NamespaceRuleGroups{
		"ns": {{Name: "g", Interval: "30s", Rules: []Rule{{Record: "live", Expr: "up"}}}},
	})

	if _, ok := m.getRuleHealth("ns", "g", "live"); !ok {
		t.Errorf("live rule health was evicted")
	}
	if _, ok := m.getRuleHealth("ns", "g", "stale"); ok {
		t.Errorf("stale rule health not evicted")
	}
	if _, ok := m.getRuleHealth("ns", "gone", "x"); ok {
		t.Errorf("health for removed group not evicted")
	}
}

func TestGetPrometheusRules_GroupEvaluationReflectsRealHealth(t *testing.T) {
	reader := &fakeReader{groups: NamespaceRuleGroups{
		"ns": {{
			Name:     "g",
			Interval: "30s",
			Rules: []Rule{
				{Record: "a", Expr: "up"},
				{Record: "b", Expr: "up"},
			},
		}},
	}}
	m := NewRuleManager(nil, reader, nil, time.Minute)

	// Never-evaluated group: must report the zero time, not "now".
	groups := m.GetPrometheusRules()
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if got := groups[0].LastEvaluation; got != (time.Time{}).UTC().Format(time.RFC3339Nano) {
		t.Errorf("un-evaluated group LastEvaluation = %q, want zero time", got)
	}
	if groups[0].EvaluationTime != 0 {
		t.Errorf("un-evaluated group EvaluationTime = %v, want 0", groups[0].EvaluationTime)
	}

	// After evaluation: group time is the latest rule time, eval time the sum.
	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := older.Add(time.Minute)
	m.setRuleHealth("ns", "g", "a", RuleHealth{Health: "ok", LastEvalTime: older, EvaluationTime: 0.2})
	m.setRuleHealth("ns", "g", "b", RuleHealth{Health: "ok", LastEvalTime: newer, EvaluationTime: 0.3})

	groups = m.GetPrometheusRules()
	if got := groups[0].LastEvaluation; got != newer.Format(time.RFC3339Nano) {
		t.Errorf("group LastEvaluation = %q, want latest rule time %q", got, newer.Format(time.RFC3339Nano))
	}
	if got := groups[0].EvaluationTime; got < 0.49 || got > 0.51 {
		t.Errorf("group EvaluationTime = %v, want sum 0.5", got)
	}
}

func TestGetPrometheusRules_RecordingOnlyWithHealth(t *testing.T) {
	eval := &fakeEvaluator{vec: sampleVec()}
	writer := &fakeWriter{}
	reader := &fakeReader{groups: NamespaceRuleGroups{
		"ns": {{
			Name:     "g",
			Interval: "30s",
			Rules: []Rule{
				{Record: "rec", Expr: "up"},
				{Alert: "Down", Expr: "up == 0"}, // must be excluded
			},
		}},
	}}
	m := NewRuleManager(eval, reader, writer, time.Minute)
	m.ctx = context.Background()
	m.evaluateInterval(context.Background(), 30*time.Second)

	groups := m.GetPrometheusRules()
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if len(groups[0].Rules) != 1 {
		t.Fatalf("expected only the recording rule, got %d", len(groups[0].Rules))
	}
	pr := groups[0].Rules[0]
	if pr.Name != "rec" || pr.Type != "recording" || pr.Health != "ok" {
		t.Errorf("prometheus rule mismatch: %+v", pr)
	}
}
