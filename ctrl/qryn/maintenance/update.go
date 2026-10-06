package maintenance

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/qryn/v5/ctrl/logger"
	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
	"github.com/metrico/qryn/v5/shared/distconfig"
)

const (
	CLUST_MODE_SINGLE      = 1
	CLUST_MODE_CLOUD       = 2
	CLUST_MODE_DISTRIBUTED = 4
)

func Update(db clickhouse.Conn, dbname string, clusterName string, mode int,
	ttlDays int, storagePolicy string, advancedSamplesOrdering string, skipUnavailableShards bool,
	logger logger.ILogger) error {
	return UpdateWithReadCluster(db, dbname, clusterName, "", "", mode,
		ttlDays, storagePolicy, advancedSamplesOrdering, skipUnavailableShards, logger)
}

func UpdateWithReadCluster(db clickhouse.Conn, dbname string, clusterName string,
	readCluster string, readSuffix string, mode int,
	ttlDays int, storagePolicy string, advancedSamplesOrdering string, skipUnavailableShards bool,
	logger logger.ILogger) error {
	checkMode := func(m int) bool { return mode&m == m }
	env := migrationEnv(dbname, clusterName, checkMode(CLUST_MODE_CLOUD), ttlDays, storagePolicy,
		advancedSamplesOrdering, skipUnavailableShards)
	var err error
	err = updateScripts(db, clusterName, 1, sql.LogScript, env, logger)
	if err != nil {
		return err
	}
	if checkMode(CLUST_MODE_DISTRIBUTED) {
		err = updateScripts(db, clusterName, 3, sql.LogDistScript, env, logger)
		if err != nil {
			return err
		}
	}
	err = updateScripts(db, clusterName, 2, sql.TracesScript, env, logger)
	if err != nil {
		return err
	}
	if checkMode(CLUST_MODE_DISTRIBUTED) {
		err = updateScripts(db, clusterName, 4, sql.TracesDistScript, env, logger)
		if err != nil {
			return err
		}
	}

	err = updateScripts(db, clusterName, 5, sql.ProfilesScript, env, logger)
	if err != nil {
		return err
	}
	if checkMode(CLUST_MODE_DISTRIBUTED) {
		err = updateScripts(db, clusterName, 6, sql.ProfilesDistScript, env, logger)
		if err != nil {
			return err
		}
	}

	// Ruler rule-group storage (recording/alerting rule groups).
	err = updateScripts(db, clusterName, 10, sql.RulesScript, env, logger)
	if err != nil {
		return err
	}
	if checkMode(CLUST_MODE_DISTRIBUTED) {
		err = updateScripts(db, clusterName, 11, sql.RulesDistScript, env, logger)
		if err != nil {
			return err
		}
	}

	err = updateScripts(db, clusterName, 12, sql.MetricsScript, env, logger)
	if err != nil {
		return err
	}
	if checkMode(CLUST_MODE_DISTRIBUTED) {
		err = updateScripts(db, clusterName, 13, sql.MetricsDistScript, env, logger)
		if err != nil {
			return err
		}
	}

	// Cross-cluster read-path tables: when a separate read cluster is configured,
	// create distributed tables that aggregate queries across multiple clusters.
	if readCluster != "" && readCluster != clusterName && checkMode(CLUST_MODE_DISTRIBUTED) {
		err = updateReadDistScripts(db, dbname, clusterName, readCluster, readSuffix,
			7, sql.LogReadDistScript, logger)
		if err != nil {
			return err
		}
		err = updateReadDistScripts(db, dbname, clusterName, readCluster, readSuffix,
			8, sql.TracesReadDistScript, logger)
		if err != nil {
			return err
		}
		err = updateReadDistScripts(db, dbname, clusterName, readCluster, readSuffix,
			9, sql.ProfilesReadDistScript, logger)
		if err != nil {
			return err
		}
	}

	err = Cleanup(db, clusterName, checkMode(CLUST_MODE_DISTRIBUTED), dbname, logger)

	return err
}

func getSQLFile(strContents string) ([]string, error) {
	var res []string
	strContents = regexp.MustCompile(`(?m)^\s+$`).ReplaceAllString(strContents, "")
	strContents = regexp.MustCompile(`(?m)^##.*$`).ReplaceAllString(strContents, "")
	_res := strings.SplitSeq(strContents, ";\n\n")
	for req := range _res {
		_req := strings.Trim(req, "\n ")
		if _req == "" {
			continue
		}
		res = append(res, _req)
	}
	return res, nil
}

func getDBExec(db clickhouse.Conn, env map[string]string, logger logger.ILogger) func(query string, args ...[]any) error {
	return func(query string, args ...[]any) error {
		name := fmt.Sprintf("tpl_%d", rand.Uint64())
		tpl, err := template.New(name).Parse(query)
		if err != nil {
			logger.Error(query)
			return err
		}
		buf := bytes.NewBuffer(nil)
		err = tpl.Execute(buf, env)
		if err != nil {
			logger.Error(query)
			return err
		}
		req := buf.String()
		err = db.Exec(context.Background(), req)
		if err != nil {
			logger.Error(req)
			return err
		}
		return nil
	}
}

func updateReadDistScripts(db clickhouse.Conn, dbname string, clusterName string,
	readCluster string, readSuffix string, k int64, file string, logger logger.ILogger) error {
	scripts, err := getSQLFile(file)
	if err != nil {
		return err
	}
	env := map[string]string{
		"DB":           dbname,
		"CLUSTER":      clusterName,
		"OnCluster":    "ON CLUSTER `" + clusterName + "`",
		"READ_CLUSTER": readCluster,
		"READ_SUFFIX":  readSuffix,
	}
	exec := getDBExec(db, env, logger)
	verTable := "ver" + distconfig.Suffix()
	var ver uint64 = 0
	if k >= 0 {
		rows, err := db.Query(context.Background(),
			fmt.Sprintf("SELECT max(ver) as ver FROM %s WHERE k = $1 FORMAT JSON", verTable), k)
		if err != nil {
			return err
		}
		for rows.Next() {
			err = rows.Scan(&ver)
			if err != nil {
				return err
			}
		}
	}
	for i := ver; i < uint64(len(scripts)); i++ {
		logger.Info(fmt.Sprintf("Upgrade read-dist v.%d to v.%d ", i, i+1))
		err = exec(scripts[i])
		if err != nil {
			logger.Error(scripts[i])
			return err
		}
		err = db.Exec(context.Background(), "INSERT INTO ver (k, ver) VALUES ($1, $2)", k, i+1)
		if err != nil {
			return err
		}
		logger.Info(fmt.Sprintf("Upgrade read-dist v.%d to v.%d ok", i, i+1))
	}
	return nil
}

// migrationEnv holds the values the migration templates are rendered with.
func migrationEnv(dbname string, clusterName string, replicated bool, ttlDays int, storagePolicy string,
	advancedSamplesOrdering string, skipUnavailableShards bool) map[string]string {
	env := map[string]string{
		"DB":                   dbname,
		"CLUSTER":              clusterName,
		"OnCluster":            " ",
		"DefaultTtlDays":       "30",
		"CREATE_SETTINGS":      "",
		"SAMPLES_ORDER_RUL":    "timestamp_ns",
		"DIST_CREATE_SETTINGS": "",
	}
	if storagePolicy != "" {
		env["CREATE_SETTINGS"] = fmt.Sprintf("SETTINGS storage_policy = '%s'", storagePolicy)
	}
	//TODO: move to the config package as it should be: os.Getenv("ADVANCED_SAMPLES_ORDERING")
	if advancedSamplesOrdering != "" {
		env["SAMPLES_ORDER_RUL"] = advancedSamplesOrdering
	}
	//TODO: move to the config package
	if skipUnavailableShards {
		env["DIST_CREATE_SETTINGS"] += " SETTINGS skip_unavailable_shards = 1"
	}
	if ttlDays != 0 {
		env["DefaultTtlDays"] = strconv.FormatInt(int64(ttlDays), 10)
	}

	if clusterName != "" {
		env["OnCluster"] = "ON CLUSTER `" + clusterName + "`"
	}
	if replicated {
		env["ReplacingMergeTree"] = "ReplicatedReplacingMergeTree"
		env["MergeTree"] = "ReplicatedMergeTree"
		env["AggregatingMergeTree"] = "ReplicatedAggregatingMergeTree"
	} else {
		env["ReplacingMergeTree"] = "ReplacingMergeTree"
		env["MergeTree"] = "MergeTree"
		env["AggregatingMergeTree"] = "AggregatingMergeTree"
	}
	return env
}

// renderScripts splits a migration file into its statements, each rendered with env.
func renderScripts(file string, env map[string]string) ([]string, error) {
	scripts, err := getSQLFile(file)
	if err != nil {
		return nil, err
	}
	for i, script := range scripts {
		tpl, err := template.New(fmt.Sprintf("script_%d", i)).Parse(script)
		if err != nil {
			return nil, err
		}
		buf := bytes.NewBuffer(nil)
		if err = tpl.Execute(buf, env); err != nil {
			return nil, err
		}
		scripts[i] = buf.String()
	}
	return scripts, nil
}

func updateScripts(db clickhouse.Conn, clusterName string, k int64, file string, env map[string]string,
	logger logger.ILogger) error {
	scripts, err := renderScripts(file, env)
	if err != nil {
		return err
	}
	verTable := "ver"
	exec := getDBExec(db, env, logger)
	err = exec(`CREATE TABLE IF NOT EXISTS ver {{.OnCluster}} (k UInt64, ver UInt64) 
ENGINE={{.ReplacingMergeTree}}(ver) ORDER BY k`)
	if err != nil {
		return err
	}
	if clusterName != "" {
		err = exec(`CREATE TABLE IF NOT EXISTS ver_dist {{.OnCluster}} (k UInt64, ver UInt64) 
ENGINE=Distributed('{{.CLUSTER}}','{{.DB}}', 'ver', rand())`)
		if err != nil {
			return err
		}
		verTable = "ver" + distconfig.Suffix()
	}
	var ver uint64 = 0
	if k >= 0 {
		rows, err := db.Query(context.Background(),
			fmt.Sprintf("SELECT max(ver) as ver FROM %s WHERE k = $1", verTable), k)
		if err != nil {
			return err
		}

		for rows.Next() {
			err = rows.Scan(&ver)
			if err != nil {
				return err
			}
		}
	}
	for i := ver; i < uint64(len(scripts)); i++ {
		logger.Info(fmt.Sprintf("Upgrade v.%d to v.%d ", i, i+1))
		err = db.Exec(context.Background(), scripts[i])
		if err != nil {
			logger.Error(scripts[i])
			return err
		}
		err = db.Exec(context.Background(), "INSERT INTO ver (k, ver) VALUES ($1, $2)", k, i+1)
		if err != nil {
			return err
		}
		logger.Info(fmt.Sprintf("Upgrade v.%d to v.%d ok", i, i+1))
	}
	return nil
}

func tableExists(db clickhouse.Conn, name string) (bool, error) {
	rows, err := db.Query(context.Background(), "SHOW TABLES")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var _name string
		err = rows.Scan(&_name)
		if err != nil {
			return false, err
		}
		if _name == name {
			return true, nil
		}
	}
	return false, nil
}

func tableEmpty(db clickhouse.Conn, name string) (bool, error) {
	rows, err := db.Query(context.Background(), fmt.Sprintf("SELECT count(1) FROM %s", name))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	rows.Next()
	var count uint64
	err = rows.Scan(&count)
	return count == 0, err
}

func isExistsAndEmpty(db clickhouse.Conn, name string) (bool, error) {
	exists, err := tableExists(db, name)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	empty, err := tableEmpty(db, name)
	return empty, err
}

func Cleanup(db clickhouse.Conn, clusterName string, distributed bool, dbname string, logger logger.ILogger) error {
	//TODO: add plugin extension
	env := map[string]string{
		"DB":             dbname,
		"CLUSTER":        clusterName,
		"OnCluster":      " ",
		"DefaultTtlDays": "30",
	}

	if clusterName != "" {
		env["OnCluster"] = "ON CLUSTER `" + clusterName + "`"
	}

	tableDeps := []struct {
		name       []string
		depsTables []string
		depsViews  []string
	}{
		{
			[]string{},
			[]string{},
			[]string{},
		},
	}

	exec := getDBExec(db, env, logger)

	for _, dep := range tableDeps {
		mainExists := false
		for _, main := range dep.name {
			existsAndEmpty, err := isExistsAndEmpty(db, main)
			if err != nil {
				return err
			}
			if existsAndEmpty {
				err = exec(fmt.Sprintf("DROP TABLE IF EXISTS %s {{.OnCluster}}", main))
				if err != nil {
					return err
				}
			}
			exists, err := tableExists(db, main)
			if err != nil {
				return err
			}
			mainExists = mainExists || exists
		}
		if mainExists {
			continue
		}
		for _, tbl := range dep.depsTables {
			err := exec(fmt.Sprintf("DROP TABLE IF EXISTS %s {{.OnCluster}}", tbl))
			if err != nil {
				return err
			}
		}
		for _, view := range dep.depsViews {
			err := db.Exec(context.Background(), fmt.Sprintf("DROP VIEW IF EXISTS %s {{.OnCluster}}", view))
			if err != nil {
				return err
			}
		}
	}
	return nil
}
