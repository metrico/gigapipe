package clickhouse_transpiler

import (
	"fmt"
	"github.com/metrico/qryn/reader/logql/logql_transpiler_v2/shared"
	sql "github.com/metrico/qryn/reader/utils/sql_select"
	"strconv"
	"time"
)

type AggregatorPlanner struct {
	Main       shared.SQLRequestPlanner
	Fn         string
	Attr       string
	CompareFn  string
	CompareVal string
	Prefix     string

	fCmpVal float64
}

func (a *AggregatorPlanner) Process(ctx *shared.PlannerContext) (sql.ISelect, error) {
	main, err := a.Main.Process(ctx)
	if err != nil {
		return nil, err
	}

	agg, err := a.getAggregator()
	if err != nil {
		return nil, err
	}

	fn, err := getComparisonFn(a.CompareFn)
	if err != nil {
		return nil, err
	}

	err = a.cmpVal()
	if err != nil {
		return nil, err
	}

	return main.AndHaving(fn(agg, sql.NewFloatVal(a.fCmpVal))), nil
}

func (a *AggregatorPlanner) cmpVal() error {
	if a.Attr == "duration" {
		cmpDuration, err := time.ParseDuration(a.CompareVal)
		if err != nil {
			return err
		}
		a.fCmpVal = float64(cmpDuration.Nanoseconds())
		return nil
	}

	var err error
	a.fCmpVal, err = strconv.ParseFloat(a.CompareVal, 64)
	return err
}

func (a *AggregatorPlanner) getAggregator() (sql.SQLObject, error) {
	switch a.Fn {
	case "count":
		return sql.NewRawObject(fmt.Sprintf("toFloat64(count(distinct %sindex_search.span_id))", a.Prefix)), nil
	case "avg":
		return sql.NewRawObject("avgIf(agg_val, isNotNull(agg_val))"), nil
	case "max":
		return sql.NewRawObject("maxIf(agg_val, isNotNull(agg_val))"), nil
	case "min":
		return sql.NewRawObject("minIf(agg_val, isNotNull(agg_val))"), nil
	case "sum":
		return sql.NewRawObject("sumIf(agg_val, isNotNull(agg_val))"), nil
	}
	return nil, &shared.NotSupportedError{"aggregator not supported: " + a.Fn}
}
