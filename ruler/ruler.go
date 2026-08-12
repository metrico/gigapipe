// Package ruler stores recording-rule groups and evaluates recording rules on
// a schedule, writing their results back into gigapipe's metrics tables.
//
// It is recording-only: alerting rules are stored but never evaluated. The
// package composes both the reader (for query evaluation) and the writer (for
// in-process write-back), the way cmd wires the unified binary.
//
// Tenancy: when federation is enabled (shared/federation) every storage call
// carries an oid, rule groups are keyed per tenant, and evaluation scopes both
// its reads and its write-back to the owning tenant. The oid is "" when
// federation is off, preserving single-tenant behavior.
package ruler

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/promql"
)

// RuleReader reads rule groups from storage. It is the read surface the manager
// and HTTP read handlers depend on. oid scopes reads to one tenant ("" when
// federation is off). GetAllRuleGroups spans all tenants (the eval loop needs
// every group) and stamps each group's owning tenant in RuleGroup.Oid.
type RuleReader interface {
	GetRuleGroup(ctx context.Context, oid, namespace, groupName string) (RuleGroup, error)
	ListRuleGroups(ctx context.Context, oid, namespace string) ([]RuleGroup, error)
	GetAllRuleGroups(ctx context.Context) (NamespaceRuleGroups, error)
}

// RuleWriter mutates rule-group storage. Deletes are soft (tombstones) so they
// win over prior versions under ReplacingMergeTree. oid is the owning tenant
// ("" when federation is off).
type RuleWriter interface {
	SetRuleGroup(ctx context.Context, oid, namespace string, group RuleGroup) error
	DeleteRuleGroup(ctx context.Context, oid, namespace, groupName string) error
	DeleteNamespace(ctx context.Context, oid, namespace string) error
}

// RuleStore is the full storage surface used by the HTTP controller.
type RuleStore interface {
	RuleReader
	RuleWriter
}

// RuleEvaluator evaluates a rule expression at an instant and returns the
// result as a Prometheus vector. LogQL and PromQL each provide one. The tenant
// to scope reads to is carried on ctx (see shared.WithOidFilter).
type RuleEvaluator interface {
	Evaluate(ctx context.Context, expr string, t time.Time) (promql.Vector, error)
}

// RecordingRuleWriter persists the result of a recording-rule evaluation back
// into gigapipe's metrics tables under the rule's record name, tagged with the
// owning tenant (oid; "" when federation is off).
type RecordingRuleWriter interface {
	Write(oid, record string, ruleLabels map[string]string, v promql.Vector) error
}
