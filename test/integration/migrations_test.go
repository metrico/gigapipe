//go:build integration

// Schema migration lane (M2, #920): gigapipe's start-up migrations run against
// an empty single-node ClickHouse database must create every table and view
// the migration scripts declare, and running them again on the same database,
// as a restart of an existing install does, must succeed and change nothing.

package integration

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"
)

// singleNodeScripts are the migration scripts applied to a single node; the
// *_dist.sql ones only run on a cluster.
var singleNodeScripts = []string{"log.sql", "traces.sql", "profiles.sql", "rules.sql"}

var createStatement = regexp.MustCompile(`(?i)CREATE\s+(?:TABLE|MATERIALIZED\s+VIEW|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?\{\{\.DB\}\}\.(\w+)`)

// declaredTables returns every table and view the given migration scripts
// create, plus ver, where the migration runner records how far each script
// has been applied (ctrl/qryn/maintenance/update.go), sorted.
func declaredTables(t *testing.T, scripts []string) []string {
	t.Helper()
	names := []string{"ver"}
	for _, script := range scripts {
		sql, err := os.ReadFile(filepath.Join("..", "..", "ctrl", "qryn", "sql", script))
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range createStatement.FindAllStringSubmatch(string(sql), -1) {
			if !slices.Contains(names, m[1]) {
				names = append(names, m[1])
			}
		}
	}
	if len(names) == 1 {
		t.Fatal("found no CREATE statements in the migration scripts")
	}
	slices.Sort(names)
	return names
}

func TestSchemaMigrations(t *testing.T) {
	db := freshDatabase(t, "m2_migrate")
	env := processEnv(db, "MODE=init_only")
	want := declaredTables(t, singleNodeScripts)

	t.Run("an empty database gets every declared table", func(t *testing.T) {
		runGigapipe(t, env, 2*time.Minute)
		if got := databaseTables(t, db); !slices.Equal(got, want) {
			t.Errorf("tables after the first run:\n got %v\nwant %v", got, want)
		}
	})

	t.Run("running the migrations again changes nothing", func(t *testing.T) {
		runGigapipe(t, env, 2*time.Minute)
		if got := databaseTables(t, db); !slices.Equal(got, want) {
			t.Errorf("tables after the second run:\n got %v\nwant %v", got, want)
		}
	})
}
