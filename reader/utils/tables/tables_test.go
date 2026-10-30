package tables

import (
	"testing"

	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/reader/logql/logql_transpiler/shared"
	"github.com/metrico/qryn/v5/reader/model"
)

func TestClusterIsTheOneTheReadTablesSpan(t *testing.T) {
	for _, c := range []struct {
		cluster, readCluster, suffix string
		homeSkips                    bool
		wantCluster                  string
		wantSkip                     bool
	}{
		{"", "", "_dist", true, "", false},
		{"", "reads", "_dist_reads", false, "", false},
		{"home", "", "_dist", false, "home", false},
		{"home", "", "_dist", true, "home", true},
		{"home", "home", "_dist_reads", false, "home", false},
		{"home", "reads", "_dist", false, "home", false},
		{"home", "reads", "_dist_reads", false, "reads", true},
	} {
		db := &model.DataDatabasesMap{Config: &config.ClokiBaseDataBase{
			Name: "db", ClusterName: c.cluster, SkipUnavailableShards: c.homeSkips}}
		ctx := populateTableNames(&shared.PlannerContext{}, db, c.readCluster, c.suffix)
		if ctx.ClusterName != c.wantCluster || ctx.SkipUnavailableShards != c.wantSkip {
			t.Errorf("cluster %q, read cluster %q, suffix %q, home skips %v: ClusterName = %q, SkipUnavailableShards = %v, want %q, %v",
				c.cluster, c.readCluster, c.suffix, c.homeSkips, ctx.ClusterName, ctx.SkipUnavailableShards, c.wantCluster, c.wantSkip)
		}
	}
}
