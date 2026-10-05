package maintenance

import (
	"testing"

	"github.com/metrico/cloki-config/config"
	"github.com/metrico/qryn/v5/shared/metricretention"
)

func TestRotationTakesTheMetricLifetimesFromTheConfiguredSettings(t *testing.T) {
	t.Setenv("METRICS_5M_DAYS", "99")
	t.Setenv("METRICS_15S_TTL_DAYS", "99")
	prev := metricretention.Configured()
	metricretention.Configure(metricretention.Settings{FiveMinuteDays: 60, RollupDays: 9})
	t.Cleanup(func() { metricretention.Configure(prev) })

	db := &config.ClokiBaseDataBase{TTLDays: 10}
	tiers, err := metricTiers(db)
	if want := (metricretention.Tiers{RawDays: 10, FiveMinuteDays: 60, HourDays: 365}); err != nil || tiers != want {
		t.Errorf("metricTiers = %+v, %v, want %+v", tiers, err, want)
	}
	if got := rollupTTLDays(db); got != 9 {
		t.Errorf("rollupTTLDays = %d, want 9", got)
	}
}

func TestEachDatabaseRotatesItsRawTierAndRollupByItsOwnTTL(t *testing.T) {
	prev := metricretention.Configured()
	metricretention.Configure(metricretention.Settings{})
	t.Cleanup(func() { metricretention.Configure(prev) })

	for ttlDays, want := range map[int]metricretention.Tiers{
		5:  {RawDays: 5, FiveMinuteDays: 30, HourDays: 365},
		90: {RawDays: 90, FiveMinuteDays: 90, HourDays: 365},
	} {
		db := &config.ClokiBaseDataBase{TTLDays: ttlDays}
		if got, err := metricTiers(db); err != nil || got != want {
			t.Errorf("ttl_days %d: metricTiers = %+v, %v, want %+v", ttlDays, got, err, want)
		}
		if got := rollupTTLDays(db); got != ttlDays {
			t.Errorf("ttl_days %d: rollupTTLDays = %d, want %d", ttlDays, got, ttlDays)
		}
	}
}
