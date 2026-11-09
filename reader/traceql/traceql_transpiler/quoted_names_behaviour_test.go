package traceql_transpiler

import (
	"context"
	stdsql "database/sql"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/tempo"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_ir"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	dbversion "github.com/metrico/qryn/v5/reader/utils/dbVersion"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
	"github.com/metrico/qryn/v5/reader/utils/tables"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

// distinctBy is the distinct first by() values of main's rows.
type distinctBy struct{ main shared.SQLRequestPlanner }

func (d distinctBy) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := d.main.Process(ctx)
	if err != nil {
		return nil, err
	}
	w := sql.NewWith(main, "m")
	return sql.NewSelect().With(w).Distinct(true).Select(sql.NewSimpleCol("toString(by_custom_0)", "v")).
		From(sql.NewWithRef(w)), nil
}

// byValues runs the instant metrics query and returns the values of its first
// by label.
func (s *searchStore) byValues(t *testing.T, query string) []string {
	t.Helper()
	q, err := traceql_ir.Parse(query)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &shared.PlannerContext{
		Ctx: context.Background(), CHDb: s.db,
		From: spanStart.Add(-time.Minute), To: spanStart.Add(time.Minute),
		Limit: searchLimit, SpansPerSpanSet: 100,
		VersionInfo: dbversion.VersionInfo{dbversion.CapStructuralQueries: 0},
	}
	distconfig.Init()
	tables.PopulateTableNames(ctx, &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{Name: s.name}})
	planner, err := clickhouse_transpiler.PlanMetrics(q, func(spans *sql.With) (sql.ISelect, error) {
		res, _, err := tempo.BuildMetricsInstantQuery(&tempo.MetricsRequest{
			Spans: spans, Fn: q.Metrics.Fn, By: q.Metrics.By,
			FromNS: ctx.From.UnixNano(), ToNS: ctx.To.UnixNano(), StepNS: int64(time.Minute),
			AttrsTable: ctx.TracesAttrsTable,
		})
		return res, err
	})
	if err != nil {
		t.Fatal(err)
	}
	var res []string
	err = clickhouse_transpiler.Query(ctx, planner.Ids, nil, distinctBy{planner.Metrics}, clickhouse_transpiler.RunOnEmptyIds,
		func(rows *stdsql.Rows) error {
			var v string
			if err := rows.Scan(&v); err != nil {
				return err
			}
			res = append(res, v)
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(res)
	return res
}

// traceIDs runs a search and returns the matched trace ids.
func (s *searchStore) traceIDs(t *testing.T, query string) []string {
	t.Helper()
	var got []string
	for _, line := range s.result(t, query, searchRun{limit: searchLimit}) {
		if id, ok := strings.CutPrefix(line, "trace "); ok {
			got = append(got, strings.Fields(id)[0])
		} else if strings.HasPrefix(line, "error:") {
			t.Fatalf("%s: %s", query, line)
		}
	}
	return got
}

func TestQuotedNamesReadTheirAttributes(t *testing.T) {
	s := openSearchStore(t, false, false)
	s.seed(t, []span{
		{trace: 1, id: 1, attrs: []string{"5xx", "foo"}},
		{trace: 2, id: 2, attrs: []string{"http status"}},
		{trace: 3, id: 3, attrs: []string{`a\\\' OR 1=1 --`}},
		{trace: 4, id: 4, attrs: []string{"other"}},
	})
	for query, want := range map[string][]string{
		`{ ."5xx" = "1" }`:             {"1"},
		`{ span."http status" = "1" }`: {"2"},
		`{ ."a\\' OR 1=1 --" = "1" }`:  {"3"},
		`{ ."nothing" = "1" }`:         nil,
	} {
		t.Run(query, func(t *testing.T) {
			if got := s.traceIDs(t, query); !slices.Equal(got, want) {
				t.Errorf("traces %q, want %q", got, want)
			}
		})
	}
	for query, want := range map[string][]string{
		`{} | rate() by (.foo)`:               {"", "1"},
		`{} | rate() by (."5xx")`:             {"", "1"},
		`{} | rate() by (span."http status")`: {"", "1"},
		`{} | rate() by (."a\\' OR 1=1 --")`:  {"", "1"},
		`{} | rate() by (.nothing)`:           {""},
	} {
		t.Run(query, func(t *testing.T) {
			if got := s.byValues(t, query); !slices.Equal(got, want) {
				t.Errorf("by values %q, want %q", got, want)
			}
		})
	}
}
