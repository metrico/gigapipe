package metricread

import (
	"testing"
	"time"

	"github.com/metrico/qryn/v5/shared/metricretention"
)

func TestSelectTier(t *testing.T) {
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	lifetimes := metricretention.Tiers{RawDays: 7, FiveMinuteDays: 30, HourDays: 365}
	day := int64(24 * time.Hour / time.Millisecond)
	const minute = int64(60000)
	// inRaw is an hour boundary two days back.
	inRaw := now.Add(-48 * time.Hour).UnixMilli()
	past := func(days int64) int64 { return inRaw - days*day }
	read := func(start, step int64, ranges ...int64) Read {
		return Read{Grid: Grid{StartMs: start, EndMs: start + 12*step, StepMs: step},
			EarliestMs: start - 10*minute, RangesMs: ranges}
	}
	instant := func(at int64) Read {
		return Read{Grid: Grid{StartMs: at, EndMs: at}, EarliestMs: at - 5*minute}
	}
	// boundary is an unaligned read whose earliest instant lies offset ms after now − days.
	boundary := func(days, offset int64) Read {
		r := read(inRaw+minute, minute, minute)
		r.EarliestMs = now.UnixMilli() - days*day + offset
		return r
	}
	engine := read(inRaw, 5*minute, 5*minute)
	engine.EngineReads = true

	for _, tc := range []struct {
		name   string
		read   Read
		forced string
		want   Tier
	}{
		{"aligned to 5m inside raw", read(inRaw, 5*minute, 5*minute, 10*minute), "", Tier5m},
		{"step off the 5m grid inside raw", read(inRaw, minute, 5*minute), "", RawTier},
		{"start off the 5m grid inside raw", read(inRaw+minute, 5*minute, 5*minute), "", RawTier},
		{"range not a multiple of 5m inside raw", read(inRaw, 5*minute, 5*minute, 7*minute), "", RawTier},
		{"a selector the engine reads inside raw", engine, "", RawTier},
		{"instant on the 5m grid inside raw", instant(inRaw + 5*minute), "", Tier5m},
		{"instant off the 5m grid inside raw", instant(inRaw + minute), "", RawTier},
		{"aligned to both grids inside raw", read(inRaw, 60*minute, 60*minute), "", Tier5m},
		{"unaligned past raw inside 5m", read(past(10)+minute, minute, 7*minute), "", Tier5m},
		{"aligned to both grids past raw inside 5m", read(past(10), 60*minute, 60*minute), "", Tier5m},
		{"aligned to both grids past 5m", read(past(40), 60*minute, 60*minute), "", Tier1h},
		{"unaligned past 5m", read(past(40)+minute, minute, minute), "", Tier1h},
		{"past the 1h tier", read(past(400), 5*minute, 5*minute), "", Tier1h},
		{"earliest read on raw's lifetime boundary", boundary(7, 0), "", RawTier},
		{"earliest read 1ms past raw's lifetime", boundary(7, -1), "", Tier5m},
		{"earliest read on the 5m tier's lifetime boundary", boundary(30, 0), "", Tier5m},
		{"earliest read 1ms past the 5m tier's lifetime", boundary(30, -1), "", Tier1h},
		{"raw forced past raw", read(past(40), 60*minute, 60*minute), "raw", RawTier},
		{"5m forced unaligned inside raw", read(inRaw+minute, minute, minute), "5m", Tier5m},
		{"5m forced past 5m", read(past(40), 60*minute, 60*minute), "5m", Tier5m},
		{"1h forced aligned to 5m inside raw", read(inRaw, 5*minute, 5*minute), "1h", Tier1h},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SelectTier(tc.read, lifetimes, now, tc.forced); got != tc.want {
				t.Errorf("got %s, want %s", got.Name, tc.want.Name)
			}
		})
	}
}
