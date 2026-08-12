package ruler

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/metrico/qryn/v5/shared/federation"
	"gopkg.in/yaml.v3"
)

// IChClient is the subset of the writer's ClickHouse client the ruler needs.
type IChClient interface {
	Exec(ctx context.Context, query string, args ...any) error
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// RulerService stores and retrieves rule groups in ClickHouse, scoped to one
// rule type ("loki" or "prom") so both rule sets coexist in one table.
//
// getClient is resolved on every call so it always uses the current client.
// gigapipe is single-tenant, so no org_id participates in any query.
type RulerService struct {
	getClient   func() IChClient
	distributed bool
	ruleType    string
}

// NewRulerService builds a RulerService. ruleType is "loki" or "prom".
func NewRulerService(getClient func() IChClient, distributed bool, ruleType string) *RulerService {
	return &RulerService{getClient: getClient, distributed: distributed, ruleType: ruleType}
}

// rulesTable selects the distributed table in a clustered deployment.
func (s *RulerService) rulesTable() string {
	if s.distributed {
		return "rules_dist"
	}
	return "rules"
}

// SetRuleGroup serializes the group to YAML and inserts it. ReplacingMergeTree,
// keyed on (namespace, group_name, type) — plus oid under federation — keeps the
// latest version.
func (s *RulerService) SetRuleGroup(ctx context.Context, oid, namespace string, group RuleGroup) error {
	// Normalize zero "for" durations so they don't round-trip as "0s".
	for i := range group.Rules {
		if group.Rules[i].For == "0s" || group.Rules[i].For == "0" {
			group.Rules[i].For = ""
		}
	}

	configYAML, err := yaml.Marshal(group)
	if err != nil {
		return fmt.Errorf("ruler: marshal rule group: %w", err)
	}

	if federation.Enabled() {
		query := fmt.Sprintf(
			"INSERT INTO %s (oid, namespace, group_name, config, updated_at, is_valid, type) VALUES (?, ?, ?, ?, now(), 1, ?)",
			s.rulesTable(),
		)
		return s.getClient().Exec(ctx, query, oid, namespace, group.Name, string(configYAML), s.ruleType)
	}
	query := fmt.Sprintf(
		"INSERT INTO %s (namespace, group_name, config, updated_at, is_valid, type) VALUES (?, ?, ?, now(), 1, ?)",
		s.rulesTable(),
	)
	return s.getClient().Exec(ctx, query, namespace, group.Name, string(configYAML), s.ruleType)
}

// DeleteRuleGroup soft-deletes a group by inserting an is_valid=0 tombstone.
// ReplacingMergeTree keeps the highest updated_at, so the tombstone wins. This
// avoids mutations, which cannot target ORDER BY key columns.
func (s *RulerService) DeleteRuleGroup(ctx context.Context, oid, namespace, groupName string) error {
	if federation.Enabled() {
		query := fmt.Sprintf(
			"INSERT INTO %s (oid, namespace, group_name, config, updated_at, is_valid, type) VALUES (?, ?, ?, '', now(), 0, ?)",
			s.rulesTable(),
		)
		return s.getClient().Exec(ctx, query, oid, namespace, groupName, s.ruleType)
	}
	query := fmt.Sprintf(
		"INSERT INTO %s (namespace, group_name, config, updated_at, is_valid, type) VALUES (?, ?, '', now(), 0, ?)",
		s.rulesTable(),
	)
	return s.getClient().Exec(ctx, query, namespace, groupName, s.ruleType)
}

// DeleteNamespace soft-deletes every group in a namespace in a single
// statement: one INSERT ... SELECT writes an is_valid=0 tombstone for each
// currently-valid group. Doing it as one statement keeps the delete atomic, so
// no group survives because it was created between a separate list and loop,
// and there is no partial state if a single tombstone fails. In distributed
// mode this routes through rules_dist, which shards by group_name, so each
// tombstone co-locates with its group for FINAL dedup.
func (s *RulerService) DeleteNamespace(ctx context.Context, oid, namespace string) error {
	if federation.Enabled() {
		query := fmt.Sprintf(
			"INSERT INTO %[1]s (oid, namespace, group_name, config, updated_at, is_valid, type) "+
				"SELECT oid, namespace, group_name, '', now(), 0, type FROM %[1]s FINAL "+
				"WHERE oid = ? AND namespace = ? AND type = ? AND is_valid = 1",
			s.rulesTable(),
		)
		return s.getClient().Exec(ctx, query, oid, namespace, s.ruleType)
	}
	query := fmt.Sprintf(
		"INSERT INTO %[1]s (namespace, group_name, config, updated_at, is_valid, type) "+
			"SELECT namespace, group_name, '', now(), 0, type FROM %[1]s FINAL "+
			"WHERE namespace = ? AND type = ? AND is_valid = 1",
		s.rulesTable(),
	)
	return s.getClient().Exec(ctx, query, namespace, s.ruleType)
}

// GetRuleGroup returns a single group by namespace and name, scoped to oid under
// federation.
func (s *RulerService) GetRuleGroup(ctx context.Context, oid, namespace, groupName string) (RuleGroup, error) {
	var (
		query string
		args  []any
	)
	if federation.Enabled() {
		query = fmt.Sprintf(
			"SELECT config FROM %s FINAL WHERE oid = ? AND namespace = ? AND group_name = ? AND type = ? AND is_valid = 1 LIMIT 1",
			s.rulesTable(),
		)
		args = []any{oid, namespace, groupName, s.ruleType}
	} else {
		query = fmt.Sprintf(
			"SELECT config FROM %s FINAL WHERE namespace = ? AND group_name = ? AND type = ? AND is_valid = 1 LIMIT 1",
			s.rulesTable(),
		)
		args = []any{namespace, groupName, s.ruleType}
	}
	rows, err := s.getClient().Query(ctx, query, args...)
	if err != nil {
		return RuleGroup{}, fmt.Errorf("ruler: query rule group: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return RuleGroup{}, fmt.Errorf("rule group not found")
	}
	var configYAML string
	if err := rows.Scan(&configYAML); err != nil {
		return RuleGroup{}, fmt.Errorf("ruler: scan rule group: %w", err)
	}
	var group RuleGroup
	if err := yaml.Unmarshal([]byte(configYAML), &group); err != nil {
		return RuleGroup{}, fmt.Errorf("ruler: unmarshal rule group: %w", err)
	}
	group.Oid = oid
	return group, nil
}

// ListRuleGroups returns all active groups in a namespace, newest first, scoped
// to oid under federation.
func (s *RulerService) ListRuleGroups(ctx context.Context, oid, namespace string) ([]RuleGroup, error) {
	var (
		query string
		args  []any
	)
	if federation.Enabled() {
		query = fmt.Sprintf(
			"SELECT config FROM %s FINAL WHERE oid = ? AND namespace = ? AND type = ? AND is_valid = 1 ORDER BY updated_at DESC",
			s.rulesTable(),
		)
		args = []any{oid, namespace, s.ruleType}
	} else {
		query = fmt.Sprintf(
			"SELECT config FROM %s FINAL WHERE namespace = ? AND type = ? AND is_valid = 1 ORDER BY updated_at DESC",
			s.rulesTable(),
		)
		args = []any{namespace, s.ruleType}
	}
	rows, err := s.getClient().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("ruler: query rule groups: %w", err)
	}
	defer rows.Close()

	var groups []RuleGroup
	for rows.Next() {
		var configYAML string
		if err := rows.Scan(&configYAML); err != nil {
			return nil, fmt.Errorf("ruler: scan rule group: %w", err)
		}
		var group RuleGroup
		if err := yaml.Unmarshal([]byte(configYAML), &group); err != nil {
			return nil, fmt.Errorf("ruler: unmarshal rule group: %w", err)
		}
		group.Oid = oid
		groups = append(groups, group)
	}
	return groups, rows.Err()
}

// GetAllRuleGroups returns all active groups for this rule type, by namespace,
// across every tenant. Under federation each group is stamped with its owning
// oid so the evaluation loop can scope reads and write-back per tenant.
func (s *RulerService) GetAllRuleGroups(ctx context.Context) (NamespaceRuleGroups, error) {
	if federation.Enabled() {
		query := fmt.Sprintf(
			"SELECT oid, namespace, config FROM %s FINAL WHERE type = ? AND is_valid = 1 ORDER BY namespace, updated_at DESC",
			s.rulesTable(),
		)
		rows, err := s.getClient().Query(ctx, query, s.ruleType)
		if err != nil {
			return nil, fmt.Errorf("ruler: query all rule groups: %w", err)
		}
		defer rows.Close()

		result := make(NamespaceRuleGroups)
		for rows.Next() {
			var oid, ns, configYAML string
			if err := rows.Scan(&oid, &ns, &configYAML); err != nil {
				return nil, fmt.Errorf("ruler: scan rule group: %w", err)
			}
			var group RuleGroup
			if err := yaml.Unmarshal([]byte(configYAML), &group); err != nil {
				return nil, fmt.Errorf("ruler: unmarshal rule group: %w", err)
			}
			group.Oid = oid
			result[ns] = append(result[ns], group)
		}
		return result, rows.Err()
	}

	query := fmt.Sprintf(
		"SELECT namespace, config FROM %s FINAL WHERE type = ? AND is_valid = 1 ORDER BY namespace, updated_at DESC",
		s.rulesTable(),
	)
	rows, err := s.getClient().Query(ctx, query, s.ruleType)
	if err != nil {
		return nil, fmt.Errorf("ruler: query all rule groups: %w", err)
	}
	defer rows.Close()

	result := make(NamespaceRuleGroups)
	for rows.Next() {
		var ns, configYAML string
		if err := rows.Scan(&ns, &configYAML); err != nil {
			return nil, fmt.Errorf("ruler: scan rule group: %w", err)
		}
		var group RuleGroup
		if err := yaml.Unmarshal([]byte(configYAML), &group); err != nil {
			return nil, fmt.Errorf("ruler: unmarshal rule group: %w", err)
		}
		result[ns] = append(result[ns], group)
	}
	return result, rows.Err()
}
