package traceql_transpiler

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/metrico/qryn/v5/reader/traceql/traceql_transpiler/clickhouse_transpiler"
	"github.com/metrico/qryn/v5/reader/utils/sql_select"
)

type portionCountRecorder struct {
	portions int64
}

func (p *portionCountRecorder) Process(_ *shared.PlannerContext, portions int64) (chan []string, error) {
	p.portions = portions
	return nil, nil
}

func (p *portionCountRecorder) SetMain(*clickhouse_transpiler.SearchPlan) {}

func TestPortionCountRoundsComplexityUpToThreshold(t *testing.T) {
	for _, tc := range []struct{ complexity, want int64 }{{10, 1}, {11, 2}, {25, 3}} {
		processor := &portionCountRecorder{}
		evaluator := &TraceQLComplexityEvaluator{threshold: 10, complexRequestProcessor: processor}
		if _, err := evaluator.ProcessComplexReq(&shared.PlannerContext{}, tc.complexity); err != nil {
			t.Fatal(err)
		}
		if processor.portions != tc.want {
			t.Errorf("complexity %d: got %d portions, want %d", tc.complexity, processor.portions, tc.want)
		}
	}
}

type simpleRequestRecorder struct {
	calls int
}

func (s *simpleRequestRecorder) Process(*shared.PlannerContext) (chan []string, error) {
	s.calls++
	return nil, nil
}

func (s *simpleRequestRecorder) SetMain(*clickhouse_transpiler.SearchPlan) {}

func TestRequestPortionsOnlyOverTheBudget(t *testing.T) {
	for _, tc := range []struct {
		estimate, portions, totalJobs int64
		simpleCalls                   int
	}{{9, 0, 1, 1}, {10, 0, 1, 1}, {11, 2, 2, 0}, {20, 2, 2, 0}, {21, 3, 3, 0}} {
		simple, portioned := &simpleRequestRecorder{}, &portionCountRecorder{}
		evaluator := &TraceQLComplexityEvaluator{
			threshold: 10, simpleRequestProcessor: simple, complexRequestProcessor: portioned,
		}
		ctx := &shared.PlannerContext{}
		if _, err := evaluator.processEstimated(ctx, tc.estimate); err != nil {
			t.Fatal(err)
		}
		if simple.calls != tc.simpleCalls || portioned.portions != tc.portions || ctx.Portions != tc.portions {
			t.Errorf("estimate %d: simple calls %d, read in %d portions, ctx.Portions %d; want %d, %d, %d",
				tc.estimate, simple.calls, portioned.portions, ctx.Portions, tc.simpleCalls, tc.portions, tc.portions)
		}
		if ctx.TotalJobs() != tc.totalJobs {
			t.Errorf("estimate %d: TotalJobs %d, want %d", tc.estimate, ctx.TotalJobs(), tc.totalJobs)
		}
	}
}

// estimateRows returns one row per count, then ends with err, or cleanly without one.
type estimateRows struct {
	counts []int64
	err    error
}

func (r *estimateRows) Columns() []string { return []string{"_count"} }
func (r *estimateRows) Close() error      { return nil }
func (r *estimateRows) Next(dest []driver.Value) error {
	if len(r.counts) == 0 {
		if r.err != nil {
			return r.err
		}
		return io.EOF
	}
	dest[0], r.counts = r.counts[0], r.counts[1:]
	return nil
}

type estimateConn struct {
	driver.Conn
	counts []int64
	err    error
}

func (c estimateConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &estimateRows{counts: slices.Clone(c.counts), err: c.err}, nil
}

func (c estimateConn) Close() error { return nil }

type estimateConnector struct {
	counts []int64
	err    error
}

func (c estimateConnector) Connect(context.Context) (driver.Conn, error) {
	return estimateConn{counts: c.counts, err: c.err}, nil
}

func (c estimateConnector) Driver() driver.Driver { return nil }

type estimateDB struct {
	model.ISqlxDB
	db *sql.DB
}

func (d estimateDB) QueryCtx(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, query, args...)
}

type selectOne struct{}

func (selectOne) Process(*shared.PlannerContext) (sql_select.ISelect, error) {
	return sql_select.NewSelect().Select(sql_select.NewRawObject("1")), nil
}

func TestEstimateIsTheFullestShard(t *testing.T) {
	for _, tc := range []struct {
		name        string
		counts      []int64
		portions    int64
		simpleCalls int
	}{
		{name: "the fullest shard is neither first nor last", counts: []int64{0, 7, 3}, portions: 4},
		{name: "the fullest shard is first", counts: []int64{7, 0}, portions: 4},
		{name: "the fullest shard follows a smaller one", counts: []int64{3, 7, 0}, portions: 4},
		{name: "no rows read at once", simpleCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sql.OpenDB(estimateConnector{counts: tc.counts})
			t.Cleanup(func() { _ = db.Close() })
			simple, portioned := &simpleRequestRecorder{}, &portionCountRecorder{}
			evaluator := &TraceQLComplexityEvaluator{
				threshold: 2, simpleRequestProcessor: simple, complexRequestProcessor: portioned,
				evaluateComplexityPlanner: selectOne{},
			}
			ctx := &shared.PlannerContext{Ctx: context.Background(), CHDb: estimateDB{db: db}}
			if _, err := evaluator.Process(ctx); err != nil {
				t.Fatal(err)
			}
			if portioned.portions != tc.portions || simple.calls != tc.simpleCalls {
				t.Errorf("read in %d portions, %d simple calls; want %d, %d",
					portioned.portions, simple.calls, tc.portions, tc.simpleCalls)
			}
		})
	}
}

func TestEstimateReadFailureFailsTheRequest(t *testing.T) {
	failure := errors.New("shard failed")
	db := sql.OpenDB(estimateConnector{counts: []int64{1}, err: failure})
	t.Cleanup(func() { _ = db.Close() })
	simple, portioned := &simpleRequestRecorder{}, &portionCountRecorder{}
	evaluator := &TraceQLComplexityEvaluator{
		threshold: 10, simpleRequestProcessor: simple, complexRequestProcessor: portioned,
		evaluateComplexityPlanner: selectOne{},
	}
	ctx := &shared.PlannerContext{Ctx: context.Background(), CHDb: estimateDB{db: db}}
	if _, err := evaluator.Process(ctx); !errors.Is(err, failure) {
		t.Errorf("Process returned %v, want %v", err, failure)
	}
	if simple.calls != 0 || portioned.portions != 0 {
		t.Errorf("read the request after the estimate failed: %d simple calls, %d portions", simple.calls, portioned.portions)
	}
}

func recordPortionedLines(t *testing.T) *[]string {
	t.Helper()
	var lines []string
	original := logPortioned
	logPortioned = func(line string) { lines = append(lines, line) }
	t.Cleanup(func() { logPortioned = original })
	return &lines
}

func TestPortionedRequestLogsOneLine(t *testing.T) {
	lines := recordPortionedLines(t)
	evaluator := &TraceQLComplexityEvaluator{threshold: 10, complexRequestProcessor: &portionCountRecorder{}}
	ctx := &shared.PlannerContext{
		Limit: 2, TraceQL: `{.a = "1"} >> {.b}`,
		From: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), To: time.Date(2026, 1, 2, 4, 4, 5, 0, time.UTC),
	}
	if _, err := evaluator.processEstimated(ctx, 25); err != nil {
		t.Fatal(err)
	}
	want := `traceql portioned query="{.a = \"1\"} >> {.b}" from=2026-01-02T03:04:05Z to=2026-01-02T04:04:05Z ` +
		`estimate=25 portion_spans=10 portions=3`
	if len(*lines) != 1 || (*lines)[0] != want {
		t.Errorf("got %q, want one line %q", *lines, want)
	}
}

func TestPortionedAutocompleteLogsOneLine(t *testing.T) {
	lines := recordPortionedLines(t)
	evaluator := &TraceQLComplexityEvaluator{threshold: 10, complexRequestProcessor: &portionCountRecorder{}}
	if _, err := evaluator.processEstimated(&shared.PlannerContext{}, 11); err != nil {
		t.Fatal(err)
	}
	if len(*lines) != 1 || !strings.Contains((*lines)[0], "portions=2") {
		t.Errorf("got %q, want one line with 2 portions", *lines)
	}
}

func TestRequestWithinTheBudgetLogsNothing(t *testing.T) {
	lines := recordPortionedLines(t)
	evaluator := &TraceQLComplexityEvaluator{threshold: 10, simpleRequestProcessor: &simpleRequestRecorder{}}
	for _, estimate := range []int64{9, 10} {
		if _, err := evaluator.processEstimated(&shared.PlannerContext{}, estimate); err != nil {
			t.Fatal(err)
		}
	}
	if len(*lines) != 0 {
		t.Errorf("got %q, want no line", *lines)
	}
}
