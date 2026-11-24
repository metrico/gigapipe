package clickhouse_transpiler

import (
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type spansStub struct{}

func (spansStub) Process(*shared.PlannerContext) (sql.ISelect, error) {
	return sql.NewSelect().
		Select(sql.NewRawObject("trace_id"), sql.NewRawObject("span_id")).
		From(sql.NewRawObject("tempo_traces_attrs_gin")), nil
}

// TestFilteredTagsAndValuesAreDistinct: the tag and value autocomplete with a
// filter query selects key or val, so a GROUP BY trace_id, span_id makes
// ClickHouse reject it with code 215.
func TestFilteredTagsAndValuesAreDistinct(t *testing.T) {
	ctx := &shared.PlannerContext{
		From:                 time.Unix(1700000000, 0),
		To:                   time.Unix(1700003600, 0),
		Limit:                100,
		TracesAttrsDistTable: "tempo_traces_attrs_gin",
	}
	planners := map[string]shared.SQLRequestPlanner{
		"tags": &SelectTagsPlanner{Main: spansStub{}},
		"values": &SelectValuesRequestPlanner{
			SelectTagsPlanner: SelectTagsPlanner{Main: spansStub{}},
			Key:               "service.name",
		},
	}
	for name, p := range planners {
		sel, err := p.Process(ctx)
		if err != nil {
			t.Fatal(err)
		}
		q, err := sel.String(sql.DefaultCtx())
		if err != nil {
			t.Fatal(err)
		}
		if !sel.GetDistinct() || len(sel.GetGroupBy()) != 0 || strings.Contains(q, "GROUP BY") {
			t.Errorf("%s: want SELECT DISTINCT without GROUP BY, got %s", name, q)
		}
	}
}
