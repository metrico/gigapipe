package maintenance

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/metrico/qryn/v5/ctrl/qryn/sql"
)

// releaseLogScriptSHA256 is the sha256 of log.sql at master d22b04d.
const releaseLogScriptSHA256 = "16a65005daed82bf0c3112e863d4e581fcf91d557820ef20c144d35f740da920"

func TestLogScriptMatchesRelease(t *testing.T) {
	sum := sha256.Sum256([]byte(sql.LogScript))
	if got := hex.EncodeToString(sum[:]); got != releaseLogScriptSHA256 {
		t.Errorf("log.sql differs from master d22b04d: sha256 %s", got)
	}
}

// TestLogRollupViewTakesAllRows checks that metrics_15s_mv aggregates every row type.
func TestLogRollupViewTakesAllRows(t *testing.T) {
	scripts, err := renderScripts(sql.LogScript, migrationEnv("cloki", "", false, 7, "", "", false, testTiers))
	if err != nil {
		t.Fatal(err)
	}
	lastCreate := ""
	for _, s := range scripts {
		if strings.Contains(s, "type != 2") {
			t.Errorf("a statement filters rows by type:\n%s", s)
		}
		if strings.HasPrefix(s, "ALTER TABLE cloki.metrics_15s_mv ") && strings.Contains(s, "MODIFY QUERY") {
			t.Errorf("a statement modifies the metrics_15s_mv query:\n%s", s)
		}
		if strings.HasPrefix(s, "CREATE MATERIALIZED VIEW IF NOT EXISTS cloki.metrics_15s_mv ") {
			lastCreate = s
		}
	}
	if lastCreate == "" {
		t.Fatal("no statement creates metrics_15s_mv")
	}
	if strings.Contains(lastCreate, "WHERE") {
		t.Errorf("the metrics_15s_mv view has a WHERE clause:\n%s", lastCreate)
	}
}
