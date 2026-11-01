package maintenance

import (
	"strings"
	"text/template"

	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
)

// metricTier is an aggregate tier: its table and its bucket width.
type metricTier struct {
	Table string
	Width string
}

var metricTiers = []metricTier{
	{Table: "metrics_5m", Width: "5 MINUTE"},
	{Table: "metrics_1h", Width: "1 HOUR"},
}

var tierAggregationTemplate = template.Must(template.New("metrics_tier").Parse(strings.TrimSuffix(sql.MetricsTierScript, "\n")))

// tierAggregation renders the tier's aggregation over input, any relation with the columns
// fingerprint, timestamp, value, prev_timestamp, prev_value and aggregate.
func tierAggregation(tier metricTier, input string) string {
	var b strings.Builder
	if err := tierAggregationTemplate.Execute(&b, struct{ Width, Input string }{tier.Width, input}); err != nil {
		panic(err)
	}
	return b.String()
}

// tierViewEnvKey names the migration env key holding the tier view's SELECT.
func tierViewEnvKey(tier metricTier) string {
	return strings.ToUpper(tier.Table) + "_SELECT"
}
