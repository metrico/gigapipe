package clickhouse_planner

import (
	"testing"

	clconfig "github.com/metrico/cloki-config"
	"github.com/metrico/qryn/v5/reader/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamSelectPlannerRemovesEmptyAndMatchAllMatchers(t *testing.T) {
	previous := config.Cloki
	config.Cloki = clconfig.New(clconfig.CLOKI_READER, nil, "", "")
	defer func() { config.Cloki = previous }()

	tests := []struct {
		name   string
		names  []string
		ops    []string
		values []string
		want   []string
	}{
		{"empty equals last", []string{"job", "env"}, []string{"=", "="}, []string{"x", ""}, []string{"job"}},
		{"empty regex last", []string{"job", "env"}, []string{"=", "=~"}, []string{"x", ""}, []string{"job"}},
		{"empty before match all", []string{"job", "env", "pod"}, []string{"=", "=", "=~"}, []string{"x", "", ".*"}, []string{"job"}},
		{"empty first", []string{"env", "job"}, []string{"=", "="}, []string{"", "x"}, []string{"job"}},
		{"match all last", []string{"job", "pod"}, []string{"=", "=~"}, []string{"x", ".*"}, []string{"job"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			planner := &StreamSelectPlanner{LabelNames: tt.names, Ops: tt.ops, Values: tt.values}

			_, err := planner.Process(&shared.PlannerContext{})

			require.NoError(t, err)
			assert.Equal(t, tt.want, planner.LabelNames)
			assert.Len(t, planner.Ops, len(tt.want))
			assert.Len(t, planner.Values, len(tt.want))
		})
	}
}
