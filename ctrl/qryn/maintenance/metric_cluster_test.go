package maintenance

import (
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
)

// metricWrappers maps each metric table to its sharding key on a cluster.
var metricWrappers = map[string]string{
	"metric_samples_in":  "fingerprint",
	"metric_samples":     "fingerprint",
	"metric_series":      "fingerprint",
	"metric_exemplars":   "fingerprint",
	"metrics_5m":         "fingerprint",
	"metrics_1h":         "fingerprint",
	"metric_metadata":    "cityHash64(name)",
	"metric_label_names": "rand()",
}

func render(t *testing.T, file string, env map[string]string) []string {
	t.Helper()
	scripts, err := renderScripts(file, env)
	if err != nil {
		t.Fatal(err)
	}
	return scripts
}

var columnLine = regexp.MustCompile(`(?m)^\s+` + "`?" + `(\w+)` + "`?" + `\s+(\S.*?)(?:\s+CODEC\(.*\))?,?$`)

// columns returns the name and type of each column a CREATE TABLE statement declares.
func columns(t *testing.T, create string) []string {
	t.Helper()
	_, body, ok := strings.Cut(create, "(\n")
	body, _, ok2 := strings.Cut(body, "\n) ENGINE")
	if !ok || !ok2 {
		t.Fatalf("no column list in:\n%s", create)
	}
	var res []string
	for _, m := range columnLine.FindAllStringSubmatch(body, -1) {
		res = append(res, m[1]+" "+m[2])
	}
	return res
}

func TestEveryMetricTableHasADistributedWrapperOnACluster(t *testing.T) {
	env := migrationEnv("cloki", "c1", false, 7, "", "", true)
	local := render(t, sql.MetricsScript, env)
	dist := render(t, sql.MetricsDistScript, env)
	for table, key := range metricWrappers {
		s := statement(t, dist, table+"_dist")
		if !strings.Contains(s, "cloki."+table+"_dist ON CLUSTER `c1` (") {
			t.Errorf("%s_dist is not created on the cluster:\n%s", table, s)
		}
		engine := "ENGINE = Distributed('c1', 'cloki', '" + table + "', " + key + ") SETTINGS skip_unavailable_shards = 1"
		if !strings.HasSuffix(strings.TrimSuffix(strings.Join(strings.Fields(s), " "), ";"), engine) {
			t.Errorf("%s_dist: want %q:\n%s", table, engine, s)
		}
		if got, want := columns(t, s), columns(t, statement(t, local, table)); !slices.Equal(got, want) {
			t.Errorf("%s_dist columns %v, want those of %s: %v", table, got, table, want)
		}
	}
	var creates int
	for _, s := range dist {
		if strings.HasPrefix(s, "CREATE ") {
			creates++
		}
	}
	if creates != len(metricWrappers) {
		t.Errorf("the wrapper script creates %d tables, want %d", creates, len(metricWrappers))
	}
	backfill := "INSERT INTO cloki.metric_label_names_dist (label, first_seen, last_seen)\n" +
		"SELECT label, min(first_seen) AS first_seen, max(last_seen) AS last_seen\n" +
		"FROM cloki.metric_series_dist\nARRAY JOIN mapKeys(labels) AS label\nGROUP BY label\n" +
		"SETTINGS insert_distributed_sync = 1"
	if !slices.ContainsFunc(dist, func(s string) bool { return strings.TrimSuffix(s, ";") == backfill }) {
		t.Errorf("no backfill of every shard's label names:\n%s", strings.Join(dist, "\n\n"))
	}
}

func TestMetricTablesReplicateUnderCloudExceptTheStagingTable(t *testing.T) {
	scripts := render(t, sql.MetricsScript, migrationEnv("cloki", "c1", true, 7, "", "", false))
	for table := range metricWrappers {
		s := statement(t, scripts, table)
		replicated := strings.Contains(s, "ENGINE = Replicated")
		if table == "metric_samples_in" {
			if replicated || !strings.Contains(s, "ENGINE = Null") {
				t.Errorf("the staging table is not Null:\n%s", s)
			}
			continue
		}
		if !replicated {
			t.Errorf("%s is not replicated under Cloud:\n%s", table, s)
		}
	}
}

func TestMetricTablesHaveReadClusterVariants(t *testing.T) {
	local := render(t, sql.MetricsScript, migrationEnv("cloki", "c1", false, 7, "", "", false))
	scripts := render(t, sql.LogReadDistScript, map[string]string{
		"DB": "cloki", "CLUSTER": "c1", "OnCluster": "ON CLUSTER `c1`",
		"READ_CLUSTER": "rc", "READ_SUFFIX": "_rd",
	})
	for table, key := range metricWrappers {
		if table == "metric_samples_in" {
			continue
		}
		s := statement(t, scripts, table+"_rd")
		engine := "ENGINE = Distributed('rc', 'cloki', '" + table + "', " + key + ") SETTINGS skip_unavailable_shards = 1"
		if !strings.HasSuffix(strings.TrimSuffix(strings.Join(strings.Fields(s), " "), ";"), engine) {
			t.Errorf("%s_rd: want %q:\n%s", table, engine, s)
		}
		if got, want := columns(t, s), columns(t, statement(t, local, table)); !slices.Equal(got, want) {
			t.Errorf("%s_rd columns %v, want those of %s: %v", table, got, table, want)
		}
	}
}

func TestClusterMetricTablesTakeTheStoragePolicyAndNoTTLAtCreation(t *testing.T) {
	for _, cloud := range []bool{false, true} {
		scripts := render(t, sql.MetricsScript, migrationEnv("cloki", "c1", cloud, 7, "cold_policy", "", true))
		for _, table := range metricDataTables {
			s := statement(t, scripts, table)
			if !strings.HasSuffix(strings.TrimSuffix(s, ";"), "SETTINGS storage_policy = 'cold_policy'") {
				t.Errorf("cloud=%v: %s is created without the storage policy:\n%s", cloud, table, s)
			}
			if strings.Contains(s, "TTL") || strings.Contains(s, "ttl_only_drop_parts") {
				t.Errorf("cloud=%v: %s sets its TTL at creation:\n%s", cloud, table, s)
			}
		}
	}
}
