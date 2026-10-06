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

func bounds(units []importUnit) []string {
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

func TestRawChunksAreWholeDaysNewestFirstThenTheDayOfH0(t *testing.T) {
	plan := planImport(at("2026-10-02 09:00"), at("2026-09-29 00:00"), time.Time{}, 30)
	sameStrings(t, "chunks", bounds(plan.chunks), []string{
		"10-01 00:00 10-02 00:00",
		"09-30 00:00 10-01 00:00",
		"09-29 00:00 09-30 00:00",
		"10-02 00:00 10-02 09:00",
	})
}

func TestRawChunksStartAtTheDayHoldingTheOldestRawRow(t *testing.T) {
	h0 := at("2026-10-02 09:00")
	for _, tc := range []struct {
		name   string
		oldest time.Time
		want   time.Time
	}{
		{"mid-day", at("2026-09-29 13:20"), at("2026-09-29 00:00")},
		{"at midnight, in the chunk ending there", at("2026-09-29 00:00"), at("2026-09-28 00:00")},
		{"beyond the old table's lifetime", at("2001-01-01 00:00"), at("2026-09-24 00:00")},
		{"none", time.Time{}, h0},
		{"only above H0", at("2026-10-02 09:30"), h0},
	} {
		if got := importFloor(h0, tc.oldest, 7); !got.Equal(tc.want) {
			t.Errorf("%s: floor = %s, want %s", tc.name, got, tc.want)
		}
	}
}

func TestRollupSpansAreWeeksBelowTheFloorDownToTheOldestRollupRow(t *testing.T) {
	floor := at("2026-09-29 00:00")
	plan := planImport(at("2026-10-02 09:00"), floor, at("2026-09-10 12:00"), 30)
	sameStrings(t, "spans", bounds(plan.spans), []string{
		"09-22 00:00 09-29 00:00",
		"09-15 00:00 09-22 00:00",
		"09-08 00:00 09-15 00:00",
	})
	plan = planImport(at("2026-10-02 09:00"), floor, at("2001-01-01 00:00"), 10)
	sameStrings(t, "spans within the rollup's lifetime", bounds(plan.spans), []string{
		"09-22 00:00 09-29 00:00",
		"09-15 00:00 09-22 00:00",
	})
	if plan = planImport(at("2026-10-02 09:00"), floor, time.Time{}, 30); len(plan.spans) != 0 {
		t.Errorf("spans without rollup rows = %q", bounds(plan.spans))
	}
}

func TestNoRawChunkWithoutRawRows(t *testing.T) {
	h0 := at("2026-10-02 09:00")
	if plan := planImport(h0, h0, time.Time{}, 30); len(plan.chunks) != 0 {
		t.Errorf("chunks = %q", bounds(plan.chunks))
	}
}

func TestARestartSkipsTheUnitsRecordedDoneAndRedoesAStartedOne(t *testing.T) {
	plan := planImport(at("2026-10-02 09:00"), at("2026-09-29 00:00"), at("2026-09-20 00:00"), 30)
	records := map[string]string{
		"chunk:" + "1790899200000": "done",    // (10-01, 10-02]
		"chunk:" + "1790812800000": "started", // (09-30, 10-01]
		"lease":                    "other:1",
	}
	if got := plan.chunks[0].name(); got != "chunk:1790899200000" {
		t.Fatalf("name = %s", got)
	}
	var got []string
	for _, p := range pendingUnits(append(plan.chunks, plan.spans...), records) {
		got = append(got, fmt.Sprintf("%s redo=%v", p.unit.name(), p.redo))
	}
	sameStrings(t, "pending", got, []string{
		"chunk:1790812800000 redo=true",
		"chunk:1790726400000 redo=false",
		"chunk:1790931600000 redo=false",
		"15s:1790640000000 redo=false",
		"15s:1790035200000 redo=false",
	})
}
