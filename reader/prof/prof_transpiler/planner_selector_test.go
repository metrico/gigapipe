package prof_transpiler

import (
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/prof/prof_parser"
	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

func renderMatchers(t *testing.T, query string) (global, kv []string) {
	t.Helper()
	script, err := prof_parser.Parse(query)
	if err != nil {
		t.Fatalf("Parse(%s): %v", query, err)
	}
	m, err := (&StreamSelectorPlanner{Selectors: script.Selectors}).getMatchers()
	if err != nil {
		t.Fatalf("getMatchers: %v", err)
	}
	ctx := &sql.Ctx{Params: map[string]sql.SQLObject{}, Result: map[string]sql.SQLObject{}}
	render := func(conds []sql.SQLCondition) []string {
		var res []string
		for _, c := range conds {
			s, err := c.String(ctx)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			res = append(res, s)
		}
		return res
	}
	return render(m.globalMatchers), render(m.kvMatchers)
}

// TestQuotedLabelNameMatchesTag checks a quoted label name is matched as the
// plain tag key: profile tags are stored under the ingested attribute name.
func TestQuotedLabelNameMatchesTag(t *testing.T) {
	global, kv := renderMatchers(t, `{"service.version"="1.4.0"}`)
	if len(global) != 0 || len(kv) != 1 {
		t.Fatalf("want 1 tag matcher, got global=%q kv=%q", global, kv)
	}
	if !strings.Contains(kv[0], `'service.version'`) || strings.Contains(kv[0], `"service.version"`) {
		t.Errorf("tag matcher %q must match key 'service.version'", kv[0])
	}
}

// TestQuotedServiceNameUsesColumn checks quoting a name does not change what it
// selects: a quoted service_name still matches the service_name column.
func TestQuotedServiceNameUsesColumn(t *testing.T) {
	global, kv := renderMatchers(t, `{"service_name"="checkout"}`)
	if len(global) != 1 || len(kv) != 0 {
		t.Fatalf("want 1 column matcher, got global=%q kv=%q", global, kv)
	}
	if !strings.Contains(global[0], "service_name") {
		t.Errorf("column matcher %q must match service_name", global[0])
	}
}
