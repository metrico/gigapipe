package service

import (
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
)

func TestExportStreamsValueGolden(t *testing.T) {
	odd := "a\"b\\c\nd\te<f>& \xff"
	out := make(chan []shared.LogEntry, 1)
	out <- []shared.LogEntry{
		{TimestampNS: 2, Fingerprint: 1, Labels: map[string]string{"l": odd}, Message: odd},
		{TimestampNS: 1, Fingerprint: 1, Labels: map[string]string{"l": odd}, Message: "m"},
		{TimestampNS: 3, Fingerprint: 2, Labels: map[string]string{"x": "y"}, Message: ""},
	}
	close(out)
	res := make(chan model.QueryRangeOutput)
	go (&QueryRangeService{}).exportStreamsValue(out, res)

	var got strings.Builder
	for o := range res {
		if o.Err != nil {
			t.Fatal(o.Err)
		}
		got.WriteString(o.Str)
	}
	esc := `a\"b\\c\nd\te<f>&` + " \xff"
	want := `{"status":"success","data":{"resultType":"streams","result":[` +
		`{"stream":{"l":"` + esc + `"},"values":[["2","` + esc + `"],["1","m"]]},` +
		`{"stream":{"x":"y"},"values":[["3",""]]}]}}`
	if got.String() != want {
		t.Fatalf("got  %q\nwant %q", got.String(), want)
	}
}
