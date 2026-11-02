package maintenance

import (
	"fmt"
	"testing"
	"time"
)

func at(s string) time.Time {
	t, err := time.Parse("2006-01-02 15:04", s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func bounds(units []migrationUnit) []string {
	var out []string
	for _, u := range units {
		out = append(out, u.from.Format("01-02 15:04")+" "+u.to.Format("01-02 15:04"))
	}
	return out
}

func sameStrings(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %q, want %q", what, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s = %q, want %q", what, got, want)
		}
	}
}

func ms(t time.Time) string {
	return fmt.Sprint(t.UnixMilli())
}

func rawRecord(to string) string {
	return "raw:" + ms(at(to))
}

func recoveries(units []migrationUnit) []string {
	var out []string
	for _, u := range units {
		if u.recovery {
			out = append(out, u.to.Format("01-02 15:04"))
		}
	}
	return out
}

func TestTheTopUnitEndsAtTheLastHourBoundaryBeforeTheMargin(t *testing.T) {
	for _, c := range []struct{ now, top string }{
		{"2026-10-02 09:04", "2026-10-02 08:00"},
		{"2026-10-02 09:05", "2026-10-02 09:00"},
		{"2026-10-02 09:59", "2026-10-02 09:00"},
	} {
		plan := planMigration(at(c.now), 1, 30, 5*time.Minute, time.Hour, nil, nil)
		if !plan.top.Equal(at(c.top)) {
			t.Errorf("now %s: top = %s, want %s", c.now, plan.top, c.top)
		}
		if !plan.units[0].to.Equal(at(c.top)) {
			t.Errorf("now %s: first unit ends %s, want %s", c.now, plan.units[0].to, c.top)
		}
	}
}

func TestTheFirstRunPlansWholeHoursNewestFirstDownToTheFloor(t *testing.T) {
	plan := planMigration(at("2026-10-02 09:10"), 1, 30, 5*time.Minute, time.Hour, nil, nil)
	if !plan.floor.Equal(at("2026-10-01 09:00")) {
		t.Fatalf("floor = %s", plan.floor)
	}
	got := bounds(plan.units)
	if len(got) != 24 {
		t.Fatalf("%d units: %q", len(got), got)
	}
	sameStrings(t, "newest", got[:2], []string{"10-02 08:00 10-02 09:00", "10-02 07:00 10-02 08:00"})
	sameStrings(t, "oldest", got[23:], []string{"10-01 09:00 10-01 10:00"})
	if r := recoveries(plan.units); len(r) != 0 {
		t.Errorf("recoveries on a first run: %q", r)
	}
}

func TestAFirstRunResumedAfterAFailureSkipsTheHoursDone(t *testing.T) {
	records := map[string]string{
		floorRecord:                   ms(at("2026-10-02 05:00")),
		rawRecord("2026-10-02 09:00"): unitDone,
		rawRecord("2026-10-02 08:00"): unitStarted,
	}
	plan := planMigration(at("2026-10-02 09:10"), 1, 30, 5*time.Minute, time.Hour, records, nil)
	sameStrings(t, "units", bounds(plan.units), []string{
		"10-02 07:00 10-02 08:00", "10-02 06:00 10-02 07:00", "10-02 05:00 10-02 06:00",
	})
	sameStrings(t, "recoveries", recoveries(plan.units), []string{"10-02 08:00"})
}

func TestALaterRunPlansTheHoursNotDoneAndTheLookback(t *testing.T) {
	records := map[string]string{
		floorRecord:     ms(at("2026-10-02 05:00")),
		watermarkRecord: ms(at("2026-10-02 09:00")),
	}
	for _, to := range []string{"2026-10-02 09:00", "2026-10-02 08:00", "2026-10-02 06:00"} {
		records[rawRecord(to)] = unitDone
	}
	records[rawRecord("2026-10-02 07:00")] = unitStarted
	plan := planMigration(at("2026-10-02 11:30"), 1, 30, 5*time.Minute, 90*time.Minute, records, nil)
	sameStrings(t, "units", bounds(plan.units), []string{
		"10-02 10:00 10-02 11:00",
		"10-02 09:00 10-02 10:00",
		"10-02 08:00 10-02 09:00",
		"10-02 07:00 10-02 08:00",
		"10-02 06:00 10-02 07:00",
	})
	sameStrings(t, "recoveries", recoveries(plan.units), []string{"10-02 07:00"})
	if !plan.floor.Equal(at("2026-10-02 05:00")) {
		t.Errorf("floor = %s, want the recorded one", plan.floor)
	}
}

func TestALaterRunWithEveryHourDonePlansOnlyTheLookback(t *testing.T) {
	records := map[string]string{
		floorRecord:     ms(at("2026-10-02 07:00")),
		watermarkRecord: ms(at("2026-10-02 09:00")),
	}
	for _, to := range []string{"2026-10-02 09:00", "2026-10-02 08:00"} {
		records[rawRecord(to)] = unitDone
	}
	plan := planMigration(at("2026-10-02 09:10"), 1, 30, 5*time.Minute, time.Hour, records, nil)
	sameStrings(t, "units", bounds(plan.units), []string{"10-02 08:00 10-02 09:00"})
}
