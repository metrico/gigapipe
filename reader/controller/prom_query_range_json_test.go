package controller

import (
	"net/http/httptest"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// Label values come from ClickHouse as-is, so the encoder must escape quotes
// and control characters and pass everything else, including invalid UTF-8,
// through unchanged.
const oddLabel = "a\"b\\c\nd\te<f>& \xff"

func TestWriteResponseGolden(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  parser.Value
		want string
	}{
		{
			name: "matrix",
			val: promql.Matrix{
				{Metric: labels.FromStrings("l", oddLabel), Floats: []promql.FPoint{{T: 1700000000123, F: 1.5}, {T: 1700000015000, F: 2}}},
				{Metric: labels.FromStrings("m", "x"), Floats: []promql.FPoint{{T: 1, F: 1e-7}}},
			},
			want: `{"status":"success","data":{"resultType":"matrix","result":[` +
				`{"metric":{"l":"a\"b\\c\nd\te<f>&` + " \xff" + `"},"values":[[1700000000.123,"1.5"],[1700000015,"2"]]},` +
				`{"metric":{"m":"x"},"values":[[0.001,"0.0000001"]]}]}}`,
		},
		{
			name: "vector",
			val: promql.Vector{
				{Metric: labels.FromStrings("l", oddLabel), T: 1700000000123, F: -3.25},
			},
			want: `{"status":"success","data":{"resultType":"vector","result":[` +
				`{"metric":{"l":"a\"b\\c\nd\te<f>&` + " \xff" + `"},"value":[1700000000.123,"-3.25"]}]}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			if err := writeResponse(&promql.Result{Value: tc.val}, rec); err != nil {
				t.Fatal(err)
			}
			if got := rec.Body.String(); got != tc.want {
				t.Fatalf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}
