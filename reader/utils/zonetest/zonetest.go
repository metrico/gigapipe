// Package zonetest runs tests under several time.Local zones and reads the
// date bounds a query renders.
package zonetest

import (
	"regexp"
	"testing"
	"time"

	sql "github.com/metrico/qryn/v5/reader/utils/sql_select"
)

// Zones have local dates that differ from the UTC date at some hour.
var Zones = []string{"UTC", "Asia/Tokyo", "America/Los_Angeles", "Pacific/Kiritimati", "Pacific/Pago_Pago"}

// Run runs f in a subtest per zone with time.Local set to it, and restores
// time.Local when t ends. Callers must not use t.Parallel.
func Run(t *testing.T, zones []string, f func(t *testing.T, loc *time.Location)) {
	t.Helper()
	saved := time.Local
	t.Cleanup(func() { time.Local = saved })
	for _, name := range zones {
		loc, err := time.LoadLocation(name)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(name, func(t *testing.T) {
			time.Local = loc
			f(t, loc)
		})
	}
}

var dateBoundRe = regexp.MustCompile(`date\) (>=|<=) \('([0-9-]+)'\)`)

// DateBounds returns the date literals sel bounds below and above, in order.
func DateBounds(t testing.TB, sel sql.ISelect) (lower, upper []string) {
	t.Helper()
	q, err := sel.String(sql.DefaultCtx())
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range dateBoundRe.FindAllStringSubmatch(q, -1) {
		if m[1] == ">=" {
			lower = append(lower, m[2])
		} else {
			upper = append(upper, m[2])
		}
	}
	return lower, upper
}

// AllEqual reports whether every element of s is v.
func AllEqual(s []string, v string) bool {
	for _, e := range s {
		if e != v {
			return false
		}
	}
	return true
}
