package maintenance

import (
	"bytes"
	"strings"
	"testing"
	"text/template"

	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
)

// TestScriptsResolveEveryToken renders every statement of every DDL script with
// the same environment updateScripts builds, refusing any unknown token.
//
// text/template resolves a missing map key to the value type's zero — an empty
// string — so a typo'd token silently renders as nothing and produces a broken
// CREATE TABLE that only fails at runtime, on someone's cluster.
// missingkey=error turns that into a test failure here instead.
func TestScriptsResolveEveryToken(t *testing.T) {
	env := scriptEnv("qryn", "mycluster", "mypolicy", "fingerprint, timestamp_ns",
		7, true, true)
	scripts := map[string]string{
		"log.sql":            sql.LogScript,
		"log_dist.sql":       sql.LogDistScript,
		"log_split.sql":      sql.LogSplitScript,
		"log_split_dist.sql": sql.LogSplitDistScript,
		"traces.sql":         sql.TracesScript,
		"traces_dist.sql":    sql.TracesDistScript,
		"profiles.sql":       sql.ProfilesScript,
		"profiles_dist.sql":  sql.ProfilesDistScript,
		"rules.sql":          sql.RulesScript,
		"rules_dist.sql":     sql.RulesDistScript,
	}
	for name, script := range scripts {
		assertRenders(t, name, script, env)
	}
}

// The read-dist scripts get a much smaller environment (update.go:160), so they
// must not reach for a token only scriptEnv supplies.
func TestReadDistScriptsResolveEveryToken(t *testing.T) {
	env := readDistEnv("qryn", "mycluster", "readcluster", "_read")
	scripts := map[string]string{
		"log_read_dist.sql":       sql.LogReadDistScript,
		"log_split_read_dist.sql": sql.LogSplitReadDistScript,
		"traces_read_dist.sql":    sql.TracesReadDistScript,
		"profiles_read_dist.sql":  sql.ProfilesReadDistScript,
	}
	for name, script := range scripts {
		assertRenders(t, name, script, env)
	}
}

func assertRenders(t *testing.T, name, script string, env map[string]string) {
	t.Helper()
	stmts, err := getSQLFile(script)
	if err != nil {
		t.Fatalf("%s: getSQLFile: %v", name, err)
	}
	if len(stmts) == 0 {
		t.Fatalf("%s: no statements parsed", name)
	}
	for i, stmt := range stmts {
		tpl, err := template.New(name).Option("missingkey=error").Parse(stmt)
		if err != nil {
			t.Fatalf("%s stmt %d: parse: %v", name, i, err)
		}
		buf := bytes.NewBuffer(nil)
		if err := tpl.Execute(buf, env); err != nil {
			t.Errorf("%s stmt %d: %v\n%s", name, i, err, stmt)
			continue
		}
		if strings.Contains(buf.String(), "{{") {
			t.Errorf("%s stmt %d: unresolved token in output\n%s", name, i, buf.String())
		}
	}
}

// The split file must carry the two tables the rest of the plan relies on, and
// must not create metrics_aggr_mv: that view is state-managed (see aggr.go),
// because a once-only migration cannot express enable, disable or reinterval.
func TestSplitScriptContents(t *testing.T) {
	env := scriptEnv("qryn", "", "", "timestamp_ns", 7, false, false)
	stmts, err := getSQLFile(sql.LogSplitScript)
	if err != nil {
		t.Fatalf("getSQLFile: %v", err)
	}
	// getSQLFile returns raw, unrendered statements ({{.DB}} and friends still
	// literal), so the table names below only appear after templating.
	rendered := make([]string, len(stmts))
	for i, s := range stmts {
		rendered[i] = render(t, s, env)
	}
	all := strings.Join(rendered, "\n")
	for _, want := range []string{
		"qryn.samples_logs", "qryn.samples_metrics",
		"qryn.logs_aggr", "qryn.metrics_aggr", "qryn.logs_aggr_mv",
	} {
		if !strings.Contains(all, want) {
			t.Errorf("log_split.sql does not create %s", want)
		}
	}
	if strings.Contains(all, "metrics_aggr_mv") {
		t.Error("metrics_aggr_mv must not be created by the versioned migration")
	}

	// samples_metrics carries no string column: nothing on the metrics read path
	// selects it, and dropping it stops OTLP exemplar trace ids from being
	// stored where no reader can reach them.
	metrics := statementContaining(t, rendered, "CREATE TABLE IF NOT EXISTS qryn.samples_metrics")
	if strings.Contains(metrics, "string") {
		t.Errorf("samples_metrics must have no string column:\n%s", metrics)
	}
	// metrics_aggr carries no bytes column: it is sum(length(string)), always
	// zero for metrics.
	aggr := statementContaining(t, rendered, "CREATE TABLE IF NOT EXISTS qryn.metrics_aggr")
	if strings.Contains(aggr, "bytes") {
		t.Errorf("metrics_aggr must have no bytes column:\n%s", aggr)
	}

	// The metrics ordering token must be wired, not hardcoded.
	metricsRaw := statementContaining(t, stmts, "CREATE TABLE IF NOT EXISTS {{.DB}}.samples_metrics")
	env["METRICS_ORDER_RUL"] = "MARKER_ORDER"
	rerendered := render(t, metricsRaw, env)
	if !strings.Contains(rerendered, "MARKER_ORDER") {
		t.Errorf("samples_metrics ignores METRICS_ORDER_RUL:\n%s", rerendered)
	}
}

func statementContaining(t *testing.T, stmts []string, needle string) string {
	t.Helper()
	for _, s := range stmts {
		if strings.Contains(s, needle) {
			return s
		}
	}
	t.Fatalf("no statement containing %q", needle)
	return ""
}

func render(t *testing.T, stmt string, env map[string]string) string {
	t.Helper()
	tpl, err := template.New("t").Option("missingkey=error").Parse(stmt)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	buf := bytes.NewBuffer(nil)
	if err := tpl.Execute(buf, env); err != nil {
		t.Fatalf("execute: %v", err)
	}
	return buf.String()
}
