package ruler

// Rule is a single recording or alerting rule within a RuleGroup.
//
// gigapipe only evaluates recording rules (non-empty Record). Alerting rules
// (non-empty Alert) may be accepted and stored by the write API but are never
// evaluated, since Grafana no longer manages datasource alerting rules.
type Rule struct {
	Record      string            `yaml:"record,omitempty" json:"record,omitempty"`
	Alert       string            `yaml:"alert,omitempty" json:"alert,omitempty"`
	Expr        string            `yaml:"expr" json:"expr"`
	For         string            `yaml:"for,omitempty" json:"for,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty" json:"labels,omitempty"`
	Annotations map[string]string `yaml:"annotations,omitempty" json:"annotations,omitempty"`
}

// IsRecording reports whether the rule produces a new time series and is
// therefore evaluated by the ruler.
func (r Rule) IsRecording() bool {
	return r.Record != ""
}

// RuleGroup is a named collection of rules sharing one evaluation interval.
// It is the unit the HTTP API creates, reads and deletes, serialized as YAML
// into the rules table's config column.
type RuleGroup struct {
	Name     string `yaml:"name" json:"name"`
	Interval string `yaml:"interval,omitempty" json:"interval,omitempty"`
	Rules    []Rule `yaml:"rules" json:"rules"`
}

// NamespaceRuleGroups maps a namespace to its rule groups. It is the shape the
// read API and the manager consume; gigapipe is single-tenant, so there is no
// enclosing org dimension.
type NamespaceRuleGroups map[string][]RuleGroup
