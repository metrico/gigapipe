package maintenance

import (
	"errors"
	"testing"

	"github.com/metrico/cloki-config/config"
)

func TestTheMigratorTakesEachDatabasesRawLifetime(t *testing.T) {
	for _, k := range []string{"METRICS_RAW_DAYS", "METRICS_5M_DAYS", "METRICS_1H_DAYS"} {
		t.Setenv(k, "")
	}
	m, err := PrepareMetricMigration([]config.ClokiBaseDataBase{{Name: "a", TTLDays: 5}, {Name: "b", TTLDays: 9}})
	if err != nil || len(m.rawDays) != 2 || m.rawDays[0] != 5 || m.rawDays[1] != 9 {
		t.Fatalf("raw days %v, err %v", m, err)
	}
	t.Setenv("METRICS_RAW_DAYS", "40")
	t.Setenv("METRICS_5M_DAYS", "30")
	m, err = PrepareMetricMigration([]config.ClokiBaseDataBase{{Name: "a", TTLDays: 5}})
	var refused *MigrationRefusedError
	if !errors.As(err, &refused) || m != nil {
		t.Fatalf("migration %v, err %v", m, err)
	}
}
