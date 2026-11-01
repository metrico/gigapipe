package maintenance

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Ops of the cluster check a run on a cluster makes before taking the lease.
const (
	opDistTables = "dist_tables"
	opMacros     = "macros"
	opClusters   = "clusters"
)

// legacyDistTables are the legacy _dist tables a cluster run reads.
var legacyDistTables = []string{legacySamples + distSuffix, legacySeries + distSuffix, legacyRollup + distSuffix}

var (
	distributedEngine = regexp.MustCompile(`^Distributed\(\s*(?:'((?:[^'\\]|\\.)*)'|([^,\s)]+))`)
	clusterMacro      = regexp.MustCompile(`\{([^{}]*)\}`)
)

// distributedCluster reads the cluster argument of a Distributed engine_full.
func distributedCluster(engine string) (string, bool) {
	m := distributedEngine.FindStringSubmatch(engine)
	if m == nil {
		return "", false
	}
	if m[2] != "" {
		return m[2], true
	}
	return strings.NewReplacer(`\'`, `'`, `\\`, `\`).Replace(m[1]), true
}

// requiredDistTables are the legacy _dist tables a run on a cluster refuses without.
var requiredDistTables = []string{legacySamples + distSuffix, legacySeries + distSuffix}

// checkLegacyClusters refuses a run on runCluster when a required legacy _dist table is missing, or
// when a legacy _dist table names a cluster, after expanding its macros from system.macros, that
// system.clusters does not list.
func checkLegacyClusters(ctx context.Context, exec Executor, runCluster string) error {
	quoted := make([]string, len(legacyDistTables))
	for i, t := range legacyDistTables {
		quoted[i] = quoteString(t)
	}
	type distTable struct{ name, engine string }
	var tables []distTable
	err := exec.Rows(ctx, Statement{
		Op: opDistTables,
		SQL: "SELECT name, engine_full FROM system.tables WHERE database = currentDatabase() " +
			"AND name IN (" + strings.Join(quoted, ", ") + ") ORDER BY name",
	}, func(scan func(dest ...any) error) error {
		var t distTable
		if err := scan(&t.name, &t.engine); err != nil {
			return err
		}
		tables = append(tables, t)
		return nil
	})
	if err != nil {
		return err
	}
	for _, name := range requiredDistTables {
		if !slices.ContainsFunc(tables, func(t distTable) bool { return t.name == name }) {
			return &MigrationRefusedError{Reason: fmt.Sprintf("%s does not exist; a run on cluster '%s' reads it", name, runCluster)}
		}
	}
	macros := map[string]string{}
	err = exec.Rows(ctx, Statement{Op: opMacros, SQL: "SELECT macro, substitution FROM system.macros"},
		func(scan func(dest ...any) error) error {
			var k, v string
			if err := scan(&k, &v); err != nil {
				return err
			}
			macros[k] = v
			return nil
		})
	if err != nil {
		return err
	}
	clusters := map[string]bool{}
	err = exec.Rows(ctx, Statement{Op: opClusters, SQL: "SELECT DISTINCT cluster FROM system.clusters"},
		func(scan func(dest ...any) error) error {
			var c string
			if err := scan(&c); err != nil {
				return err
			}
			clusters[c] = true
			return nil
		})
	if err != nil {
		return err
	}
	for _, t := range tables {
		named, ok := distributedCluster(t.engine)
		if !ok {
			return &MigrationRefusedError{Reason: fmt.Sprintf("%s is not a Distributed table: %s", t.name, t.engine)}
		}
		var missing []string
		cluster := clusterMacro.ReplaceAllStringFunc(named, func(m string) string {
			v, ok := macros[m[1:len(m)-1]]
			if !ok {
				missing = append(missing, m)
			}
			return v
		})
		if len(missing) > 0 {
			return &MigrationRefusedError{Reason: fmt.Sprintf("%s names cluster '%s', whose macro %s is not in system.macros",
				t.name, named, strings.Join(missing, ", "))}
		}
		if !clusters[cluster] {
			if cluster != named {
				return &MigrationRefusedError{Reason: fmt.Sprintf("%s names cluster '%s' ('%s'), which is not in system.clusters",
					t.name, named, cluster)}
			}
			return &MigrationRefusedError{Reason: fmt.Sprintf("%s names cluster '%s', which is not in system.clusters", t.name, cluster)}
		}
	}
	return nil
}
