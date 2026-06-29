package ruler

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"gopkg.in/yaml.v3"
)

type execCall struct {
	query string
	args  []any
}

// fakeClient records Exec calls; Query is unused by the write paths under test.
type fakeClient struct {
	execs []execCall
	err   error
}

func (f *fakeClient) Exec(ctx context.Context, query string, args ...any) error {
	f.execs = append(f.execs, execCall{query: query, args: args})
	return f.err
}

func (f *fakeClient) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return nil, nil
}

func newTestService(c IChClient, distributed bool, ruleType string) *RulerService {
	return NewRulerService(func() IChClient { return c }, distributed, ruleType)
}

func TestSetRuleGroup_InsertsSerializedConfig(t *testing.T) {
	c := &fakeClient{}
	svc := newTestService(c, false, "prom")

	group := RuleGroup{
		Name:     "g1",
		Interval: "30s",
		Rules:    []Rule{{Record: "job:rate", Expr: "rate(x[5m])"}},
	}
	if err := svc.SetRuleGroup(context.Background(), "ns1", group); err != nil {
		t.Fatalf("SetRuleGroup: %v", err)
	}

	if len(c.execs) != 1 {
		t.Fatalf("expected 1 exec, got %d", len(c.execs))
	}
	call := c.execs[0]
	if !containsAll(call.query, "INSERT INTO rules", "namespace", "group_name", "config", "is_valid", "type") {
		t.Errorf("unexpected query: %s", call.query)
	}

	// Args carry namespace, group name, serialized config, and rule type.
	// The serialized config must round-trip back to the original group.
	var configArg string
	var foundNs, foundType bool
	for _, a := range call.args {
		switch s := a.(type) {
		case string:
			if s == "ns1" {
				foundNs = true
			}
			if s == "prom" {
				foundType = true
			}
			if containsAll(s, "job:rate", "rate(x[5m])") {
				configArg = s
			}
		}
	}
	if !foundNs || !foundType {
		t.Errorf("namespace/type not in args: %v", call.args)
	}
	var got RuleGroup
	if err := yaml.Unmarshal([]byte(configArg), &got); err != nil {
		t.Fatalf("config not valid yaml: %v", err)
	}
	if got.Name != "g1" || len(got.Rules) != 1 || got.Rules[0].Record != "job:rate" {
		t.Errorf("config round-trip mismatch: %+v", got)
	}
}

func TestDeleteRuleGroup_WritesTombstone(t *testing.T) {
	c := &fakeClient{}
	svc := newTestService(c, false, "loki")

	if err := svc.DeleteRuleGroup(context.Background(), "ns1", "g1"); err != nil {
		t.Fatalf("DeleteRuleGroup: %v", err)
	}
	if len(c.execs) != 1 {
		t.Fatalf("expected 1 exec, got %d", len(c.execs))
	}
	// A tombstone is an INSERT with is_valid = 0, not a DELETE/mutation.
	if !containsAll(c.execs[0].query, "INSERT INTO rules", "0") {
		t.Errorf("delete should write a tombstone insert: %s", c.execs[0].query)
	}
}

func TestDeleteNamespace_SingleAtomicTombstoneInsert(t *testing.T) {
	c := &fakeClient{}
	svc := newTestService(c, false, "prom")

	if err := svc.DeleteNamespace(context.Background(), "ns1"); err != nil {
		t.Fatalf("DeleteNamespace: %v", err)
	}
	// The whole namespace must be tombstoned in one statement, not a
	// list-then-loop of separate inserts.
	if len(c.execs) != 1 {
		t.Fatalf("expected 1 exec (atomic), got %d", len(c.execs))
	}
	q := c.execs[0].query
	if !containsAll(q, "INSERT INTO rules", "SELECT", "is_valid = 1", "0") {
		t.Errorf("expected single INSERT ... SELECT tombstone statement: %s", q)
	}
	// Args carry namespace and rule type for the WHERE clause.
	var foundNs, foundType bool
	for _, a := range c.execs[0].args {
		if s, ok := a.(string); ok {
			if s == "ns1" {
				foundNs = true
			}
			if s == "prom" {
				foundType = true
			}
		}
	}
	if !foundNs || !foundType {
		t.Errorf("namespace/type not in args: %v", c.execs[0].args)
	}
}

func TestRulesTable_DistributedSwitch(t *testing.T) {
	if got := newTestService(&fakeClient{}, true, "prom").rulesTable(); got != "rules_dist" {
		t.Errorf("distributed table = %q, want rules_dist", got)
	}
	if got := newTestService(&fakeClient{}, false, "prom").rulesTable(); got != "rules" {
		t.Errorf("single table = %q, want rules", got)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !contains(s, sub) {
			return false
		}
	}
	return true
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
