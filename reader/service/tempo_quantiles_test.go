package service

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/model"
	"github.com/stretchr/testify/require"
)

// quantileRegistry serves one session.
type quantileRegistry struct{ db *model.DataDatabasesMap }

func (r quantileRegistry) GetDB(context.Context) (*model.DataDatabasesMap, error) { return r.db, nil }
func (r quantileRegistry) Run()                                                   {}
func (r quantileRegistry) Stop()                                                  {}
func (r quantileRegistry) Ping() error                                            { return nil }

func serviceOver(name string, session model.ISqlxDB) *TempoService {
	return &TempoService{ServiceData: model.ServiceData{Session: quantileRegistry{&model.DataDatabasesMap{
		Config: &config.ClokiBaseDataBase{Name: name}, Session: session}}}}
}

// fakeRows are rows of a statement the fake reader answers.
type fakeRows struct {
	columns []string
	rows    [][]driver.Value
	i       int
}

func (r *fakeRows) Columns() []string { return r.columns }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

// fakeReader answers a metrics query's statements: exemplar rows when the SQL
// reads the exemplar span, series rows when it selects a value, nothing
// otherwise.
type fakeReader struct {
	series, exemplars [][]driver.Value
	db                *stdsql.DB
}

func newFakeReader(series, exemplars [][]driver.Value) *fakeReader {
	f := &fakeReader{series: series, exemplars: exemplars}
	f.db = stdsql.OpenDB(fakeConnector{f})
	return f
}

func (f *fakeReader) answer(query string) driver.Rows {
	switch {
	case strings.Contains(query, "any(("):
		columns := []string{"ts", "trace_id", "duration_ns", "span_ts"}
		if len(f.exemplars) > 0 {
			columns = make([]string, len(f.exemplars[0]))
		}
		return &fakeRows{columns: columns, rows: f.exemplars}
	case strings.Contains(query, " as value"):
		return &fakeRows{columns: make([]string, len(f.series[0])), rows: f.series}
	}
	return &fakeRows{columns: []string{"none"}}
}

func (f *fakeReader) GetName() string { return "fake-quantiles" }
func (f *fakeReader) QueryCtx(ctx context.Context, query string, args ...any) (*stdsql.Rows, error) {
	return f.db.QueryContext(ctx, query, args...)
}
func (f *fakeReader) ExecCtx(ctx context.Context, query string, args ...any) error {
	_, err := f.db.ExecContext(ctx, query, args...)
	return err
}
func (f *fakeReader) Conn(ctx context.Context) (*stdsql.Conn, error) { return f.db.Conn(ctx) }
func (f *fakeReader) Begin() (*stdsql.Tx, error)                     { return f.db.Begin() }
func (f *fakeReader) Close()                                         { f.db.Close() }

type fakeConnector struct{ f *fakeReader }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { return fakeConn(c), nil }
func (c fakeConnector) Driver() driver.Driver                        { return fakeDriver{} }

type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, driver.ErrSkip }

type fakeConn struct{ f *fakeReader }

func (c fakeConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c fakeConn) Close() error                        { return nil }
func (c fakeConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (c fakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	return c.f.answer(query), nil
}

var (
	quantileStart = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	quantileReq   = func(query string) *model.MetricsQueryRequest {
		return &model.MetricsQueryRequest{Query: query, From: quantileStart,
			To: quantileStart.Add(19 * time.Second), Step: 20 * time.Second}
	}
)

// A query-wide row of [.25, .75, 1] and one bucket for groups a and b.
func twoGroupSeries() [][]driver.Value {
	ts := quantileStart.UnixNano()
	return [][]driver.Value{
		{int64(0), []float64{0.25, 0.75, 1}, uint64(1), ""},
		{ts, []float64{0.25, 0.5, 0.9}, uint64(0), "a"},
		{ts, []float64{0.3, 0.7, 1}, uint64(0), "b"},
	}
}

func exemplarRow(group string, seconds float64) []driver.Value {
	ts := quantileStart.UnixNano()
	row := []driver.Value{ts, "t", int64(seconds * 1e9), ts}
	if group != "" {
		row = append(row, group)
	}
	return row
}

// seriesExemplars is each exemplar value of a series, in seconds.
func seriesExemplars(t *testing.T, s model.MetricsTimeSeries) []float64 {
	t.Helper()
	res := []float64{}
	for _, e := range s.Exemplars {
		b, err := json.Marshal(e)
		require.NoError(t, err)
		var v struct {
			Value float64 `json:"value"`
		}
		require.NoError(t, json.Unmarshal(b, &v))
		res = append(res, v.Value)
	}
	return res
}

func rangeSeries(t *testing.T, svc *TempoService, query string) map[string]model.MetricsTimeSeries {
	t.Helper()
	res, err := svc.MetricsQueryRange(context.Background(), quantileReq(query))
	require.NoError(t, err)
	byLabels := make(map[string]model.MetricsTimeSeries, len(res.Series))
	for _, s := range res.Series {
		byLabels[s.PromLabels] = s
	}
	require.Len(t, byLabels, len(res.Series))
	return byLabels
}

func TestQuantileListFansOutPerGroup(t *testing.T) {
	svc := serviceOver("fake", newFakeReader(twoGroupSeries(), nil))
	got := rangeSeries(t, svc, `{} | quantile_over_time(duration, .5, .95, .99) by (.x)`)
	want := map[string]float64{
		`{.x="a", p="0.5"}`: 0.25, `{.x="a", p="0.95"}`: 0.5, `{.x="a", p="0.99"}`: 0.9,
		`{.x="b", p="0.5"}`: 0.3, `{.x="b", p="0.95"}`: 0.7, `{.x="b", p="0.99"}`: 1,
	}
	require.Len(t, got, len(want))
	for labels, value := range want {
		require.Contains(t, got, labels)
		s := got[labels]
		require.Len(t, s.Samples, 1, labels)
		require.Equal(t, value, s.Samples[0].Value, labels)
		require.Len(t, s.Labels, 2, labels)
		require.Equal(t, ".x", s.Labels[0].Key)
		require.Equal(t, "p", s.Labels[1].Key)
		require.NotNil(t, s.Labels[1].Value.DoubleValue)
	}
}

func TestQuantileWithoutByCarriesOnlyP(t *testing.T) {
	ts := quantileStart.UnixNano()
	svc := serviceOver("fake", newFakeReader([][]driver.Value{
		{int64(0), []float64{0, 1}, uint64(1)},
		{ts, []float64{0, 1}, uint64(0)},
	}, nil))
	got := rangeSeries(t, svc, `{} | quantile_over_time(duration, 0, 1)`)
	require.Len(t, got, 2)
	require.Equal(t, 0.0, got[`{p="0"}`].Samples[0].Value)
	require.Equal(t, 1.0, got[`{p="1"}`].Samples[0].Value)
	require.Len(t, got[`{p="0"}`].Labels, 1)
	require.NotNil(t, got[`{p="0"}`].Labels[0].Value.DoubleValue)
}

// A span in the bucket at time 0 is a sample, not the query-wide row.
func TestQuantileBucketAtTimeZeroIsASample(t *testing.T) {
	svc := serviceOver("fake", newFakeReader([][]driver.Value{
		{int64(0), []float64{1}, uint64(1)},
		{int64(0), []float64{0.5}, uint64(0)},
	}, nil))
	res, err := svc.MetricsQueryRange(context.Background(), &model.MetricsQueryRequest{
		Query: `{} | quantile_over_time(duration, .5)`,
		From:  time.Unix(0, 0), To: time.Unix(19, 0), Step: 20 * time.Second})
	require.NoError(t, err)
	require.Len(t, res.Series, 1)
	require.Equal(t, 0.5, res.Series[0].Samples[0].Value)
}

func TestExemplarGoesToNearestQueryWideQuantile(t *testing.T) {
	svc := serviceOver("fake", newFakeReader(twoGroupSeries(), [][]driver.Value{
		exemplarRow("a", 0.3),
		// Nearest .75 query-wide; a's own bucket has .9 nearer than .5.
		exemplarRow("a", 0.72),
		exemplarRow("a", 0.95),
		// Halfway between .25 and .75.
		exemplarRow("b", 0.5),
		exemplarRow("b", 0.8),
	}))
	got := rangeSeries(t, svc, `{} | quantile_over_time(duration, .5, .95, .99) by (.x)`)
	for labels, want := range map[string][]float64{
		`{.x="a", p="0.5"}`:  {0.3},
		`{.x="a", p="0.95"}`: {0.72},
		`{.x="a", p="0.99"}`: {0.95},
		`{.x="b", p="0.5"}`:  {0.5},
		`{.x="b", p="0.95"}`: {0.8},
		`{.x="b", p="0.99"}`: {},
	} {
		require.Equal(t, want, seriesExemplars(t, got[labels]), labels)
	}
}

func TestExemplarWithoutByGoesToNearestQuantile(t *testing.T) {
	ts := quantileStart.UnixNano()
	svc := serviceOver("fake", newFakeReader([][]driver.Value{
		{int64(0), []float64{0.25, 1}, uint64(1)},
		{ts, []float64{0.25, 1}, uint64(0)},
	}, [][]driver.Value{exemplarRow("", 0.3), exemplarRow("", 0.9)}))
	got := rangeSeries(t, svc, `{} | quantile_over_time(duration, .5, .99)`)
	require.Equal(t, []float64{0.3}, seriesExemplars(t, got[`{p="0.5"}`]))
	require.Equal(t, []float64{0.9}, seriesExemplars(t, got[`{p="0.99"}`]))
}

// exemplars=N caps each series after exemplars are assigned to it.
func TestExemplarCapIsPerQuantileSeries(t *testing.T) {
	svc := serviceOver("fake", newFakeReader(twoGroupSeries(), [][]driver.Value{
		exemplarRow("a", 0.2), exemplarRow("a", 0.3), exemplarRow("a", 0.7), exemplarRow("a", 0.8),
	}))
	got := rangeSeries(t, svc, `{} | quantile_over_time(duration, .5, .95, .99) by (.x) with(exemplars=1)`)
	require.Len(t, seriesExemplars(t, got[`{.x="a", p="0.5"}`]), 1)
	require.Len(t, seriesExemplars(t, got[`{.x="a", p="0.95"}`]), 1)
}

// Exemplar labels are the by-labels, without p.
func TestExemplarLabelsCarryNoP(t *testing.T) {
	svc := serviceOver("fake", newFakeReader(twoGroupSeries(), [][]driver.Value{exemplarRow("a", 0.3)}))
	got := rangeSeries(t, svc, `{} | quantile_over_time(duration, .5, .95, .99) by (.x)`)
	b, err := json.Marshal(got[`{.x="a", p="0.5"}`].Exemplars)
	require.NoError(t, err)
	require.Contains(t, string(b), `".x"`)
	require.NotContains(t, string(b), `"p"`)
}

// A by-label named p is told from the quantile label by position.
func TestByLabelNamedP(t *testing.T) {
	ts := quantileStart.UnixNano()
	svc := serviceOver("fake", newFakeReader([][]driver.Value{
		{int64(0), []float64{0.25, 1}, uint64(1), ""},
		{ts, []float64{0.25, 1}, uint64(0), "a"},
	}, [][]driver.Value{exemplarRow("a", 0.3), exemplarRow("a", 0.9), exemplarRow("z", 0.3)}))
	got := rangeSeries(t, svc, `{} | quantile_over_time(duration, .5, .99) by (p)`)
	require.Equal(t, []float64{0.3}, seriesExemplars(t, got[`{p="a", p="0.5"}`]))
	require.Equal(t, []float64{0.9}, seriesExemplars(t, got[`{p="a", p="0.99"}`]))

	instant := serviceOver("fake", newFakeReader([][]driver.Value{{[]float64{0.25, 1}, "a"}}, nil))
	res, err := instant.MetricsQueryInstant(context.Background(),
		quantileReq(`{} | quantile_over_time(duration, .5, .99) by (p)`))
	require.NoError(t, err)
	require.Len(t, res.Series, 2)
	require.Equal(t, "a", res.Series[0].Labels[0].Value.StringValue)
	require.Equal(t, 0.5, *res.Series[0].Labels[1].Value.DoubleValue)
}

func TestInstantQuantileListHasTheRangeShape(t *testing.T) {
	svc := serviceOver("fake", newFakeReader([][]driver.Value{
		{[]float64{0.25, 0.5, 0.9}, "a"},
		{[]float64{0.3, 0.7, 1}, "b"},
	}, nil))
	res, err := svc.MetricsQueryInstant(context.Background(),
		quantileReq(`{} | quantile_over_time(duration, .5, .95, .99) by (.x)`))
	require.NoError(t, err)
	type row struct {
		x     string
		p     float64
		value float64
	}
	var got []row
	for _, s := range res.Series {
		require.Len(t, s.Labels, 2)
		require.Equal(t, ".x", s.Labels[0].Key)
		require.Equal(t, "p", s.Labels[1].Key)
		got = append(got, row{s.Labels[0].Value.StringValue, *s.Labels[1].Value.DoubleValue, s.Value})
	}
	require.True(t, slices.Equal(got, []row{
		{"a", 0.5, 0.25}, {"a", 0.95, 0.5}, {"a", 0.99, 0.9},
		{"b", 0.5, 0.3}, {"b", 0.95, 0.7}, {"b", 0.99, 1},
	}), "%v", got)

	noBy := serviceOver("fake", newFakeReader([][]driver.Value{{[]float64{0.25, 1}}}, nil))
	res, err = noBy.MetricsQueryInstant(context.Background(), quantileReq(`{} | quantile_over_time(duration, .5, 1)`))
	require.NoError(t, err)
	require.Len(t, res.Series, 2)
	require.Len(t, res.Series[1].Labels, 1)
	require.Equal(t, "p", res.Series[1].Labels[0].Key)
	require.Equal(t, 1.0, *res.Series[1].Labels[0].Value.DoubleValue)
}

// Other functions keep one value: named when ungrouped, every exemplar on its group.
func TestOtherFunctionsKeepOneValuePerSeries(t *testing.T) {
	ts := quantileStart.UnixNano()
	ungrouped := serviceOver("fake", newFakeReader([][]driver.Value{{ts, 2.5}}, [][]driver.Value{exemplarRow("", 0.3)}))
	got := rangeSeries(t, ungrouped, `{} | rate()`)
	require.Equal(t, 2.5, got[`{__name__="rate"}`].Samples[0].Value)
	require.Len(t, got[`{__name__="rate"}`].Exemplars, 1)

	grouped := serviceOver("fake", newFakeReader([][]driver.Value{{ts, 0.5, "a"}, {ts, 0.75, "b"}},
		[][]driver.Value{exemplarRow("a", 0.3), exemplarRow("a", 0.9), exemplarRow("b", 0.3)}))
	got = rangeSeries(t, grouped, `{} | avg_over_time(duration) by (.x)`)
	require.Equal(t, []float64{0.3, 0.9}, seriesExemplars(t, got[`{.x="a"}`]))
	require.Equal(t, []float64{0.3}, seriesExemplars(t, got[`{.x="b"}`]))

	instant := serviceOver("fake", newFakeReader([][]driver.Value{{2.5}}, nil))
	res, err := instant.MetricsQueryInstant(context.Background(), quantileReq(`{} | rate()`))
	require.NoError(t, err)
	require.Len(t, res.Series, 1)
	require.Empty(t, res.Series[0].Labels)
	require.Equal(t, 2.5, res.Series[0].Value)
}
