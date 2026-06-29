// Package ruler stores recording-rule groups and evaluates recording rules on
// a schedule, writing their results back into gigapipe's metrics tables.
//
// It is single-tenant and recording-only: alerting rules are stored but never
// evaluated. The package composes both the reader (for query evaluation) and
// the writer (for in-process write-back), the way cmd wires the unified binary.
package ruler

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql"
)

// RuleReader reads rule groups from storage. It is the read surface the manager
// and HTTP read handlers depend on.
type RuleReader interface {
	GetRuleGroup(ctx context.Context, namespace, groupName string) (RuleGroup, error)
	ListRuleGroups(ctx context.Context, namespace string) ([]RuleGroup, error)
	GetAllRuleGroups(ctx context.Context) (NamespaceRuleGroups, error)
}

// RuleWriter mutates rule-group storage. Deletes are soft (tombstones) so they
// win over prior versions under ReplacingMergeTree.
type RuleWriter interface {
	SetRuleGroup(ctx context.Context, namespace string, group RuleGroup) error
	DeleteRuleGroup(ctx context.Context, namespace, groupName string) error
	DeleteNamespace(ctx context.Context, namespace string) error
}

// RuleStore is the full storage surface used by the HTTP controller.
type RuleStore interface {
	RuleReader
	RuleWriter
}

// RuleEvaluator evaluates a rule expression at an instant and returns the
// result as a Prometheus vector. LogQL and PromQL each provide one.
type RuleEvaluator interface {
	Evaluate(ctx context.Context, expr string, t time.Time) (promql.Vector, error)
}

// RecordingRuleWriter persists the result of a recording-rule evaluation back
// into gigapipe's metrics tables under the rule's record name.
type RecordingRuleWriter interface {
	Write(record string, ruleLabels map[string]string, v promql.Vector) error
}
