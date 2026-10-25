package clickhouse_planner

import (
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// ExemplarsPlanner reads back the exemplars recorded alongside metric samples.
//
// The OTLP metrics ingest path stores the first exemplar's trace id in the
// sample's `string` column, a column otherwise only used by log lines (see
// firstExemplarTraceID in writer/utils/unmarshal/otlp_metrics.go). So an
// exemplar here is a metric sample whose string column is not empty, and
// reading one back means resolving the label set of the series it belongs to.
//
// Only the trace id is recoverable. The ingest path keeps neither the
// exemplar's own value nor its span id or filtered attributes, so the value
// reported against an exemplar is the value of the data point that carried it.
type ExemplarsPlanner struct {
	Fingerprints shared.SQLRequestPlanner
}

// fingerprintIn bounds a table to the fingerprints the selector matched.
//
// This is a plain IN, on a cluster as much as on a single node, and that is
// deliberate: `fp_sel` reads the *local* time_series_gin table, not the
// distributed one (see StreamSelectPlanner). Sharding is by fingerprint, so a
// series' gin rows live on the same shard as its samples, and every shard
// therefore resolves the local subquery correctly and completely.
//
// GLOBAL IN here would be silently wrong, not merely slower: it evaluates the
// subquery once on the initiator, against only the initiator's local gin rows,
// and every series that hashed to another shard vanishes from the result with
// no error. The same reasoning is why FingerprintFilterPlanner uses a plain IN.
//
// This is the opposite of the labels-join case in #702, where GLOBAL IN *is*
// required -- there both sides of the IN are distributed tables, which
// distributed_product_mode=deny rejects with error 288. Nothing here joins two
// distributed tables through an IN.
func fingerprintIn(left sql.SQLObject, with *sql.With) sql.SQLCondition {
	return sql.NewIn(left, sql.NewWithRef(with))
}

func (e *ExemplarsPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	fpSel, err := e.Fingerprints.Process(ctx)
	if err != nil {
		return nil, err
	}
	withFP := sql.NewWith(fpSel, "fp_sel")

	// The exemplar-bearing samples. `string != ''` is the whole selectivity of
	// this query: exemplars are a tiny fraction of metric samples, so this
	// runs before any label resolution rather than after it.
	samples := sql.NewSelect().
		With(withFP).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("timestamp_ns", "timestamp_ns"),
			sql.NewSimpleCol("value", "value"),
			sql.NewSimpleCol("string", "trace_id")).
		From(sql.NewRawObject(ctx.SamplesDistTableName)).
		AndWhere(
			fingerprintIn(sql.NewRawObject("fingerprint"), withFP),
			sql.Ge(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.From.UnixNano())),
			sql.Lt(sql.NewRawObject("timestamp_ns"), sql.NewIntVal(ctx.To.UnixNano())),
			sql.Neq(sql.NewRawObject("string"), sql.NewStringVal("")))
	withMain := sql.NewWith(samples, "main")

	timeSeries := sql.NewSelect().
		Distinct(true).
		With(withFP).
		Select(
			sql.NewSimpleCol("fingerprint", "fingerprint"),
			sql.NewSimpleCol("labels", "labels")).
		From(sql.NewRawObject(ctx.TimeSeriesDistTableName)).
		AndWhere(
			fingerprintIn(sql.NewRawObject("fingerprint"), withFP),
			GetTypes(ctx))
	withTS := sql.NewWith(timeSeries, "_time_series")

	// ANY LEFT rather than INNER: many exemplars map onto one series, and an
	// ANY INNER JOIN would keep only one pair per fingerprint, silently
	// dropping every exemplar of a series but the first. Fingerprints whose
	// labels have not been written yet are dropped by the length filter.
	return sql.NewSelect().
		With(withMain, withTS).
		Select(
			sql.NewSimpleCol("main.fingerprint", "fingerprint"),
			sql.NewSimpleCol("_time_series.labels", "labels"),
			sql.NewSimpleCol("main.trace_id", "trace_id"),
			sql.NewSimpleCol("main.value", "value"),
			sql.NewSimpleCol("main.timestamp_ns", "timestamp_ns")).
		From(sql.NewWithRef(withMain)).
		Join(sql.NewJoin(
			"ANY LEFT ",
			sql.NewWithRef(withTS),
			sql.Eq(sql.NewRawObject("main.fingerprint"), sql.NewRawObject("_time_series.fingerprint")))).
		AndWhere(sql.Gt(sql.NewRawObject("length(_time_series.labels)"), sql.NewIntVal(0))).
		// Ordering by fingerprint lets the caller group rows into series while
		// reading, without buffering the whole result to sort it.
		OrderBy(
			sql.NewRawObject("fingerprint"),
			sql.NewRawObject("timestamp_ns")), nil
}
