package metricretention

import (
	"strings"
	"testing"
)

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

// tiersFrom reads the settings from vars and resolves them for a database of samplesDays.
func tiersFrom(samplesDays int, vars map[string]string) (Tiers, error) {
	s, err := FromEnv(env(vars))
	if err != nil {
		return Tiers{}, err
	}
	return s.Tiers(samplesDays)
}

func TestRetentionTiersDefaultToSamplesDaysThirtyAndAYear(t *testing.T) {
	got, err := tiersFrom(10, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := Tiers{RawDays: 10, FiveMinuteDays: 30, HourDays: 365}
	if got != want {
		t.Errorf("Tiers = %+v, want %+v", got, want)
	}
}

func TestRetentionTiersReadTheirDaySettings(t *testing.T) {
	got, err := tiersFrom(7, map[string]string{
		"METRICS_RAW_DAYS": "3",
		"METRICS_5M_DAYS":  "14",
		"METRICS_1H_DAYS":  "90",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := Tiers{RawDays: 3, FiveMinuteDays: 14, HourDays: 90}
	if got != want {
		t.Errorf("Tiers = %+v, want %+v", got, want)
	}
}

func TestACoarserTierShorterThanAFinerOneIsRejected(t *testing.T) {
	for name, vars := range map[string]map[string]string{
		"5m shorter than raw":          {"METRICS_RAW_DAYS": "40", "METRICS_5M_DAYS": "30"},
		"5m shorter than SAMPLES_DAYS": {"METRICS_RAW_DAYS": "", "METRICS_5M_DAYS": "5"},
		"1h shorter than 5m":           {"METRICS_5M_DAYS": "400", "METRICS_1H_DAYS": "365"},
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := tiersFrom(7, vars); err == nil {
				t.Errorf("Tiers = %+v, want an error", got)
			}
		})
	}
}

func TestEqualTierLifetimesAreAccepted(t *testing.T) {
	got, err := tiersFrom(7, map[string]string{"METRICS_5M_DAYS": "7", "METRICS_1H_DAYS": "7"})
	if err != nil {
		t.Fatal(err)
	}
	want := Tiers{RawDays: 7, FiveMinuteDays: 7, HourDays: 7}
	if got != want {
		t.Errorf("Tiers = %+v, want %+v", got, want)
	}
}

func TestATierCannotBeSwitchedOffOrGivenPartialDays(t *testing.T) {
	for _, name := range []string{"METRICS_RAW_DAYS", "METRICS_5M_DAYS", "METRICS_1H_DAYS"} {
		for _, v := range []string{"0", "-1", "1.5", "off"} {
			t.Run(name+"="+v, func(t *testing.T) {
				if got, err := tiersFrom(7, map[string]string{name: v}); err == nil {
					t.Errorf("Tiers = %+v, want an error", got)
				}
			})
		}
	}
}

func TestUnsetCoarserTiersFollowALongerFinerTierUp(t *testing.T) {
	for samplesDays, want := range map[int]Tiers{
		3650: {RawDays: 3650, FiveMinuteDays: 3650, HourDays: 3650},
		60:   {RawDays: 60, FiveMinuteDays: 60, HourDays: 365},
	} {
		got, err := tiersFrom(samplesDays, nil)
		if err != nil {
			t.Fatalf("SAMPLES_DAYS=%d: %v", samplesDays, err)
		}
		if got != want {
			t.Errorf("SAMPLES_DAYS=%d: Tiers = %+v, want %+v", samplesDays, got, want)
		}
	}
}

func TestAnUnsetHourTierFollowsASetFiveMinuteTierUp(t *testing.T) {
	got, err := tiersFrom(7, map[string]string{"METRICS_5M_DAYS": "400"})
	if err != nil {
		t.Fatal(err)
	}
	want := Tiers{RawDays: 7, FiveMinuteDays: 400, HourDays: 400}
	if got != want {
		t.Errorf("Tiers = %+v, want %+v", got, want)
	}
}

func TestADatabaseWithoutTTLDaysTakesTheSamplesDaysDefault(t *testing.T) {
	got, err := tiersFrom(0, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := Tiers{RawDays: 7, FiveMinuteDays: 30, HourDays: 365}
	if got != want {
		t.Errorf("Tiers = %+v, want %+v", got, want)
	}
}

func TestEachDatabaseResolvesTheSharedSettingsAgainstItsOwnTTL(t *testing.T) {
	s, err := FromEnv(env(map[string]string{"METRICS_1H_DAYS": "400"}))
	if err != nil {
		t.Fatal(err)
	}
	for samplesDays, want := range map[int]Tiers{
		3:  {RawDays: 3, FiveMinuteDays: 30, HourDays: 400},
		60: {RawDays: 60, FiveMinuteDays: 60, HourDays: 400},
	} {
		if got, err := s.Tiers(samplesDays); err != nil || got != want {
			t.Errorf("Tiers(%d) = %+v, %v, want %+v", samplesDays, got, err, want)
		}
	}
}

func TestSettingsCarryTheReadTierAndTheRollupLifetime(t *testing.T) {
	s, err := FromEnv(env(map[string]string{"METRICS_READ_TIER": "5m", "METRICS_15S_TTL_DAYS": "9"}))
	if err != nil {
		t.Fatal(err)
	}
	if s.ReadTier != "5m" || s.Rollup(7) != 9 {
		t.Errorf("ReadTier = %q, Rollup(7) = %d, want 5m and 9", s.ReadTier, s.Rollup(7))
	}
	if unset := (Settings{}); unset.Rollup(7) != 7 {
		t.Errorf("Rollup(7) without METRICS_15S_TTL_DAYS = %d, want the database's 7", unset.Rollup(7))
	}
}

func TestARollupLifetimeMustBeAPositiveNumberOfDays(t *testing.T) {
	for _, v := range []string{"0", "-1", "1.5", "off"} {
		if s, err := FromEnv(env(map[string]string{"METRICS_15S_TTL_DAYS": v})); err == nil {
			t.Errorf("METRICS_15S_TTL_DAYS=%s: FromEnv = %+v, want an error", v, s)
		}
	}
}

func TestErrorsNameTheGigapipeSettings(t *testing.T) {
	for want, vars := range map[string]map[string]string{
		"GIGAPIPE_METRICS_1H_DAYS: invalid value":                                  {"METRICS_1H_DAYS": "off"},
		"METRICS_15S_TTL_DAYS: invalid value":                                      {"METRICS_15S_TTL_DAYS": "off"},
		"GIGAPIPE_METRICS_5M_DAYS (20) must not be shorter than the raw tier (30)": {"METRICS_5M_DAYS": "20"},
		"GIGAPIPE_METRICS_1H_DAYS (40) must not be shorter than GIGAPIPE_METRICS_5M_DAYS (50)": {
			"METRICS_5M_DAYS": "50", "METRICS_1H_DAYS": "40"},
	} {
		if _, err := tiersFrom(30, vars); err == nil || !strings.HasPrefix(err.Error(), want) {
			t.Errorf("error = %v, want one starting %q", err, want)
		}
	}
}
