//go:build integration

// PromQL served from the aggregate tiers. At an aligned read the 5m tier reproduces the
// probe's raw figures; the readers on GIGAPIPE_5M_URL and GIGAPIPE_1H_URL run with
// METRICS_READ_TIER forcing their tier.

package integration

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/metrico/qryn/v5/writer/utils/proto/prompb"
)

func tierURL(env, def string) string {
	if u := os.Getenv(env); u != "" {
		return strings.TrimRight(u, "/")
	}
	return def
}

var (
	forced5m = tierURL("GIGAPIPE_5M_URL", "http://localhost:3101")
	forced1h = tierURL("GIGAPIPE_1H_URL", "http://localhost:3102")
)

func waitReadyAt(t *testing.T, base string) {
	t.Helper()
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
		resp, err := http.Get(base + "/api/v1/status/buildinfo")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
	}
	t.Fatalf("gigapipe at %s never became ready", base)
}

// writeTierProbe writes the probe at an hour boundary three hours back, inside raw's lifetime.
func writeTierProbe(t *testing.T) (string, int64) {
	t.Helper()
	waitReadyAt(t, forced5m)
	waitReadyAt(t, forced1h)
	name, t0 := writeProbeAt(t, time.Now().Add(-3*time.Hour).Truncate(time.Hour).UnixMilli())
	eventually(t, fmt.Sprintf("SELECT sum(count) FROM metrics_5m WHERE fingerprint IN "+
		"(SELECT fingerprint FROM metric_series WHERE name = '%s')", name), "53")
	return name, t0
}

// tablesRead returns the metric tables ClickHouse read for the queries carrying marker.
func tablesRead(t *testing.T, marker string) string {
	t.Helper()
	clickhouseQuery(t, "SYSTEM FLUSH LOGS")
	return clickhouseQuery(t, fmt.Sprintf("SELECT arrayStringConcat(arraySort(groupUniqArrayArray("+
		"arrayFilter(x -> x LIKE '%%.metric%%' AND x NOT LIKE '%%.metric_series', tables))), ',') "+
		"FROM system.query_log WHERE type = 'QueryFinish' AND query LIKE '%%%s%%' "+
		"AND query NOT LIKE '%%system.query_log%%'", marker))
}

func TestPromQLTierReadReproducesTheProbe(t *testing.T) {
	name, t0 := writeTierProbe(t)
	end := t0 + 600000
	sel := func(tag string) string { return fmt.Sprintf(`%s{instance="", job!="%s"}`, name, tag) }
	at := func(s series, ts int64) string {
		if len(s) != 1 {
			return fmt.Sprintf("%d series", len(s))
		}
		for _, pts := range s {
			if v, ok := pts[ts]; ok {
				return fmt.Sprintf("%.7g", v)
			}
		}
		return "absent"
	}
	for _, reader := range []struct{ name, base string }{{"forced", forced5m}, {"aligned", baseURL()}} {
		for _, tc := range []struct {
			tag, query string
			instant    bool
			ts         int64
			want       string
		}{
			{"r5", "rate(%s[5m]%s)", false, t0 + 300000, "0.06666667"},
			{"r5", "rate(%s[5m]%s)", false, end, "0.04561404"},
			{"r10", "rate(%s[10m]%s)", false, end, "0.05641026"},
			{"r10i", "rate(%s[10m]%s)", true, end, "0.05641026"},
			{"inc10", "increase(%s[10m]%s)", false, end, "33.84615"},
			{"inc10i", "increase(%s[10m]%s)", true, end, "33.84615"},
			{"x", "%s%s", false, t0 + 300000, "17"},
			{"x", "%s%s", false, end, "12"},
			{"x5i", "%s%s", true, t0 + 300000, "17"},
			{"x10i", "%s%s", true, end, "12"},
		} {
			tag := fmt.Sprintf("tier_%s_%s_%d", reader.name, tc.tag, tc.ts)
			query := fmt.Sprintf(tc.query, sel(tag), "")
			var got, raw series
			if tc.instant {
				got = instantFrom(t, reader.base, query, tc.ts, 0)
				raw = instantQuery(t, fmt.Sprintf(tc.query, name+`{instance=""}`, " offset 1m"), tc.ts, 60000)
			} else {
				got = rangeFrom(t, reader.base, query, t0, end, 300000, 0)
				raw = rangeQuery(t, fmt.Sprintf(tc.query, name+`{instance=""}`, ""), t0, end, 0)
			}
			if v, r := at(got, tc.ts), at(raw, tc.ts); v != tc.want || r != tc.want {
				t.Errorf("%s %s at +%ds = %s, raw %s, want %s", reader.name, tc.query, (tc.ts-t0)/1000, v, r, tc.want)
			}
			if tables := tablesRead(t, tag); tables != "cloki.metrics_5m" {
				t.Errorf("%s %s at +%ds read %q, want the 5m tier only", reader.name, tc.query, (tc.ts-t0)/1000, tables)
			}
		}
	}

	// The 1h tier's bucket ending at 01:00 holds the last sample at 00:10, 50 minutes back.
	tag := "tier_1h_x"
	if got := instantFrom(t, forced1h, sel(tag), t0+3600000, 0); len(got) != 0 {
		t.Errorf("1h tier at 01:00 = %v, want nothing", got)
	}
	if got := instantFrom(t, forced1h, fmt.Sprintf("increase(%s[1h])", sel(tag)), t0+3600000, 0); at(got, t0+3600000) == "absent" {
		t.Errorf("1h tier increase[1h] at 01:00 = %v, want a value", got)
	}
	if tables := tablesRead(t, tag); tables != "cloki.metrics_1h" {
		t.Errorf("1h tier read %q, want the 1h tier only", tables)
	}
}

// approximateFromATier lists the functions §5.4 of the spec serves approximately from a tier.
var approximateFromATier = map[string]bool{"irate": true, "idelta": true}

// largeSamples rises by step every 15s over (t0, t0+10m] from base, large next to its spread.
func largeSamples(t0 int64, base, step float64) []*prompb.Sample {
	var res []*prompb.Sample
	for k := int64(0); k < 40; k++ {
		res = append(res, &prompb.Sample{Timestamp: t0 + (k+1)*15000, Value: base + step*float64(k)})
	}
	return res
}

func TestPromQLForcedTierEqualsTheEngineOverRawSamplesAtAlignedReads(t *testing.T) {
	name, t0 := writeTierProbe(t)
	large := func(instance string, base, step float64) *prompb.TimeSeries {
		return &prompb.TimeSeries{Labels: []*prompb.Label{{Name: "__name__", Value: name},
			{Name: "instance", Value: instance}, {Name: "job", Value: "probe"}}, Samples: largeSamples(t0, base, step)}
	}
	remoteWrite(t, large("1e9", 1e9, 1), large("ts", 1790796900, 15))
	eventually(t, fmt.Sprintf("SELECT sum(count) FROM metrics_5m WHERE fingerprint IN "+
		"(SELECT fingerprint FROM metric_series WHERE name = '%s')", name), "133")
	start, end := t0-300000, t0+900000
	type expr struct {
		query string
		exact bool
	}
	var queries []expr
	for _, fn := range []string{"rate", "increase", "delta", "irate", "idelta", "resets", "changes",
		"count_over_time", "sum_over_time", "min_over_time", "max_over_time", "avg_over_time",
		"stddev_over_time", "stdvar_over_time", "present_over_time", "last_over_time", "absent_over_time"} {
		for _, r := range []string{"1m", "5m", "10m"} {
			// A 1m range is widened to the 5m bucket (§5.3).
			queries = append(queries, expr{fmt.Sprintf("%s(%s[%s]%%s)", fn, name, r), !approximateFromATier[fn] && r != "1m"})
		}
	}
	queries = append(queries, expr{name + "%s", true})
	for _, q := range append([]expr(nil), queries...) {
		for _, agg := range []string{"sum by (job)", "max without (instance)", "min", "count", "avg by (instance)"} {
			queries = append(queries, expr{agg + " (" + q.query + ")", q.exact})
		}
	}
	var differ []string
	for _, q := range queries {
		pushed, raw := fmt.Sprintf(q.query, ""), fmt.Sprintf(q.query, " offset 1m")
		diffs := []string{sameSeries(rangeFrom(t, forced5m, pushed, start, end, 300000, 0),
			rangeFrom(t, baseURL(), raw, start, end, 300000, 60000))}
		for _, at := range []int64{t0 + 300000, t0 + 600000} {
			diffs = append(diffs, sameSeries(instantFrom(t, forced5m, pushed, at, 0), instantQuery(t, raw, at, 60000)))
		}
		for _, d := range diffs {
			if d == "" {
				continue
			}
			if q.exact {
				t.Errorf("%s from the 5m tier: %s", pushed, d)
			} else {
				differ = append(differ, pushed+": "+d)
			}
			break
		}
	}
	t.Logf("%d of %d expressions served from the 5m tier, all approximate there, differ from raw:\n%s",
		len(differ), len(queries), strings.Join(differ, "\n"))
}
