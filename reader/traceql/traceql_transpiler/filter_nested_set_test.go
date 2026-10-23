package traceql_transpiler

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/reader/model"
)

// spanIDs runs a TraceQL search and returns the listed span ids, sorted.
func spanIDs(t *testing.T, db model.ISqlxDB, query string) []string {
	t.Helper()
	var ids []string
	for _, trace := range searchTraces(t, db, query) {
		for _, set := range trace.SpanSets {
			for _, span := range set.Spans {
				ids = append(ids, span.SpanID)
			}
		}
	}
	sort.Strings(ids)
	return ids
}

// assertIDs checks the spans a query lists.
func assertIDs(t *testing.T, db model.ISqlxDB, query string, want ...string) {
	t.Helper()
	got := spanIDs(t, db, query)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("%s: got %v, want %v", query, got, want)
	}
}

const (
	root  = "0000000000000001"
	child = "0000000000000002"
)

// numberTrace stores the numbering of trace 1: root 1/4/-1, child 2/3/1.
func numberTrace(t *testing.T) model.ISqlxDB {
	t.Helper()
	conn, db := openTraces(t)
	execSQL(t, conn, fmt.Sprintf(`INSERT INTO tempo_traces_nested_set (trace_id, span_id, date, left, right, parent, version)
		VALUES (unhex('%032x'), unhex('%016x'), '2026-10-01', 1, 4, -1, 1),
		(unhex('%032x'), unhex('%016x'), '2026-10-01', 2, 3, 1, 1)`, 1, 1, 1, 2))
	return db
}

// The root filter finds the roots of a trace that is not numbered.
func TestRootFilterFindsRootsOfUnnumberedTrace(t *testing.T) {
	_, db := openTraces(t)
	assertIDs(t, db, `{nestedSetParent < 0}`, root)
	assertIDs(t, db, `{nestedSetParent = -1}`, root)
	assertIDs(t, db, `{nestedSetParent < 0 && .a = 1}`, root)
}

// The root filter narrows only the operand that holds it.
func TestRootFilterNarrowsItsOwnOperand(t *testing.T) {
	_, db := openTraces(t)
	assertIDs(t, db, `{nestedSetParent < 0} &>> {true}`, root, child)
	assertIDs(t, db, `{nestedSetParent < 0 && .a = 1} &>> {true}`, root, child)
	assertIDs(t, db, `{nestedSetParent < 0} >> {true}`, child)
}

// Nested-set filters compare the stored values.
func TestNestedSetFiltersReadStoredValues(t *testing.T) {
	db := numberTrace(t)
	for query, want := range map[string][]string{
		`{nestedSetLeft > 1}`:                        {child},
		`{nestedSetLeft >= 1}`:                       {root, child},
		`{nestedSetRight < 4}`:                       {child},
		`{nestedSetRight <= 4}`:                      {root, child},
		`{nestedSetParent != -1}`:                    {child},
		`{nestedSetParent = 1}`:                      {child},
		`{nestedSetParent > -1}`:                     {child},
		`{nestedSetRight - nestedSetLeft = 1}`:       {child},
		`{nestedSetRight - nestedSetLeft > 1}`:       {root},
		`{nestedSetLeft = 1 && .a = 1}`:              {root},
		`{nestedSetLeft > 1 && .a = 1}`:              nil,
		`{nestedSetLeft > 1 || .a = 1}`:              {root, child},
		`{nestedSetParent < 0 && nestedSetLeft = 1}`: {root},
	} {
		assertIDs(t, db, query, want...)
	}
}

// Spans of a trace not yet numbered read 0/0/0 in filters, except that the
// root filter reads the parent id.
func TestNestedSetFiltersOfUnnumberedTrace(t *testing.T) {
	_, db := openTraces(t)
	assertIDs(t, db, `{nestedSetLeft = 0 && nestedSetRight = 0}`, root, child)
	assertIDs(t, db, `{nestedSetParent != -1}`, root, child)
	assertIDs(t, db, `{nestedSetParent = 0}`, root, child)
	assertIDs(t, db, `{nestedSetLeft > 0}`)
}

// spansetAttrs runs a TraceQL search and returns each spanset as its sorted
// span ids and its attributes in protobuf JSON.
func spansetAttrs(t *testing.T, db model.ISqlxDB, query string) []string {
	t.Helper()
	var res []string
	for _, trace := range searchTraces(t, db, query) {
		for _, set := range trace.SpanSets {
			var ids []string
			for _, span := range set.Spans {
				ids = append(ids, span.SpanID)
			}
			sort.Strings(ids)
			attrs, err := json.Marshal(set.Attributes)
			if err != nil {
				t.Fatal(err)
			}
			res = append(res, strings.Join(ids, ",")+" "+string(attrs))
		}
	}
	sort.Strings(res)
	return res
}

// by() groups on stored values, keyed by ints; aggregates read them too.
func TestNestedSetGroupKeysAndAggregates(t *testing.T) {
	db := numberTrace(t)
	for query, want := range map[string][]string{
		`{} | by(nestedSetParent)`: {
			root + ` [{"key":"by(nestedSetParent)","value":{"intValue":"-1"}}]`,
			child + ` [{"key":"by(nestedSetParent)","value":{"intValue":"1"}}]`,
		},
		`{} | max(nestedSetRight) > 3`: {
			root + "," + child + ` [{"key":"max(nestedSetRight)","value":{"intValue":"4"}}]`,
		},
	} {
		got := spansetAttrs(t, db, query)
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("%s:\ngot  %v\nwant %v", query, got, want)
		}
	}
}
