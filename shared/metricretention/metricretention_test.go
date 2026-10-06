package metricretention

import "testing"

func env(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func TestRetentionTiersDefaultToSamplesDaysThirtyAndAYear(t *testing.T) {
	got, err := FromEnv(10, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	want := Tiers{RawDays: 10, FiveMinuteDays: 30, HourDays: 365}
	if got != want {
		t.Errorf("FromEnv = %+v, want %+v", got, want)
	}
}

func TestRetentionTiersReadTheirDaySettings(t *testing.T) {
	got, err := FromEnv(7, env(map[string]string{
		"METRICS_RAW_DAYS": "3",
		"METRICS_5M_DAYS":  "14",
		"METRICS_1H_DAYS":  "90",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := Tiers{RawDays: 3, FiveMinuteDays: 14, HourDays: 90}
	if got != want {
		t.Errorf("FromEnv = %+v, want %+v", got, want)
	}
}

func TestACoarserTierShorterThanAFinerOneIsRejected(t *testing.T) {
	for name, vars := range map[string]map[string]string{
		"5m shorter than raw":          {"METRICS_RAW_DAYS": "40", "METRICS_5M_DAYS": "30"},
		"5m shorter than SAMPLES_DAYS": {"METRICS_RAW_DAYS": "", "METRICS_5M_DAYS": "5"},
		"1h shorter than 5m":           {"METRICS_5M_DAYS": "400", "METRICS_1H_DAYS": "365"},
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := FromEnv(7, env(vars)); err == nil {
				t.Errorf("FromEnv = %+v, want an error", got)
			}
		})
	}
}

func TestEqualTierLifetimesAreAccepted(t *testing.T) {
	got, err := FromEnv(7, env(map[string]string{"METRICS_5M_DAYS": "7", "METRICS_1H_DAYS": "7"}))
	if err != nil {
		t.Fatal(err)
	}
	want := Tiers{RawDays: 7, FiveMinuteDays: 7, HourDays: 7}
	if got != want {
		t.Errorf("FromEnv = %+v, want %+v", got, want)
	}
}

func TestATierCannotBeSwitchedOffOrGivenPartialDays(t *testing.T) {
	for _, name := range []string{"METRICS_RAW_DAYS", "METRICS_5M_DAYS", "METRICS_1H_DAYS"} {
		for _, v := range []string{"0", "-1", "1.5", "off"} {
			t.Run(name+"="+v, func(t *testing.T) {
				if got, err := FromEnv(7, env(map[string]string{name: v})); err == nil {
					t.Errorf("FromEnv = %+v, want an error", got)
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
		got, err := FromEnv(samplesDays, env(nil))
		if err != nil {
			t.Fatalf("SAMPLES_DAYS=%d: %v", samplesDays, err)
		}
		if got != want {
			t.Errorf("SAMPLES_DAYS=%d: FromEnv = %+v, want %+v", samplesDays, got, want)
		}
	}
}

func TestAnUnsetHourTierFollowsASetFiveMinuteTierUp(t *testing.T) {
	got, err := FromEnv(7, env(map[string]string{"METRICS_5M_DAYS": "400"}))
	if err != nil {
		t.Fatal(err)
	}
	want := Tiers{RawDays: 7, FiveMinuteDays: 400, HourDays: 400}
	if got != want {
		t.Errorf("FromEnv = %+v, want %+v", got, want)
	}
}
