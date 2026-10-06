// Package envalias lets deployments configure gigapipe with GIGAPIPE_-prefixed
// environment variables while the configuration layer still reads the older
// QRYN_ and CLOKI_ names.
//
// Two separate consumers read those older names, which is why this is done by
// rewriting the environment rather than by changing each call site:
//
//   - cloki-config binds every config-file key to an environment variable
//     through viper, using the prefix in Setting.EnvPrefix (default "QRYN").
//     Viper supports exactly one prefix, so the new names cannot simply be
//     added alongside the old ones.
//   - a handful of call sites in this repository read specific names directly
//     with os.Getenv: the basic-auth credentials in cmd, the ruler settings in
//     ruler/router, and the writer's log-file overrides.
//
// Apply covers both by giving the old names the value of the new ones before
// anything reads either.
package envalias

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

// Prefix is the prefix new deployments should use.
const Prefix = "GIGAPIPE_"

// irregular maps the GIGAPIPE_ names whose legacy equivalents do not follow the
// GIGAPIPE_X -> QRYN_X rule. The credentials appear under both legacy prefixes
// because cmd reads QRYN_LOGIN first and then lets CLOKI_LOGIN override it, so
// feeding only one of them would let a stale legacy value win. The writer's
// log-file overrides have no underscore after the prefix at all. The retention
// tier settings and METRICS_READ_TIER are read unprefixed.
var irregular = map[string][]string{
	"GIGAPIPE_LOGIN":             {"QRYN_LOGIN", "CLOKI_LOGIN"},
	"GIGAPIPE_PASSWORD":          {"QRYN_PASSWORD", "CLOKI_PASSWORD"},
	"GIGAPIPE_APPLOGPATH":        {"CLOKIAPPLOGPATH"},
	"GIGAPIPE_APPLOGNAME":        {"CLOKIAPPLOGNAME"},
	"GIGAPIPE_METRICS_RAW_DAYS":  {"METRICS_RAW_DAYS"},
	"GIGAPIPE_METRICS_5M_DAYS":   {"METRICS_5M_DAYS"},
	"GIGAPIPE_METRICS_1H_DAYS":   {"METRICS_1H_DAYS"},
	"GIGAPIPE_METRICS_READ_TIER": {"METRICS_READ_TIER"},
}

// replacement is what stands in for a deprecated name: the GIGAPIPE_ name that
// replaces it, or, for a setting nothing reads, what takes its place.
type replacement struct {
	name    string
	retired string
}

// deprecated lists the legacy names worth warning about individually when they
// are set without their GIGAPIPE_ equivalent, and the settings nothing reads.
// Config keys reached through viper's prefix are not enumerated: there are too
// many, and they are not deprecated one by one.
var deprecated = map[string]replacement{
	"QRYN_LOGIN":                        {name: "GIGAPIPE_LOGIN"},
	"CLOKI_LOGIN":                       {name: "GIGAPIPE_LOGIN"},
	"QRYN_PASSWORD":                     {name: "GIGAPIPE_PASSWORD"},
	"CLOKI_PASSWORD":                    {name: "GIGAPIPE_PASSWORD"},
	"CLOKIAPPLOGPATH":                   {name: "GIGAPIPE_APPLOGPATH"},
	"CLOKIAPPLOGNAME":                   {name: "GIGAPIPE_APPLOGNAME"},
	"QRYN_RULER_ENABLED":                {name: "GIGAPIPE_RULER_ENABLED"},
	"QRYN_RULER_POLL_INTERVAL":          {name: "GIGAPIPE_RULER_POLL_INTERVAL"},
	"QRYN_RULER_MAX_LOGQL_RESULT_BYTES": {name: "GIGAPIPE_RULER_MAX_LOGQL_RESULT_BYTES"},
	"METRICS_RAW_DAYS":                  {name: "GIGAPIPE_METRICS_RAW_DAYS"},
	"METRICS_5M_DAYS":                   {name: "GIGAPIPE_METRICS_5M_DAYS"},
	"METRICS_1H_DAYS":                   {name: "GIGAPIPE_METRICS_1H_DAYS"},
	"METRICS_READ_TIER":                 {name: "GIGAPIPE_METRICS_READ_TIER"},
	"METRICS_15S_ENABLED":               {retired: "the metric retention tiers always exist"},
	"COMPAT_4_0_19":                     {retired: "nothing replaces it"},
}

// Apply copies every GIGAPIPE_-prefixed variable onto the legacy name or names
// the configuration layer reads, overwriting whatever was there. Overwriting is
// deliberate: when a deployment sets both, the GIGAPIPE_ value is the one that
// was meant.
//
// A deployment that sets no GIGAPIPE_ variable is left exactly as it was, so
// existing configurations keep working untouched.
//
// It returns one message per legacy variable that is set without its GIGAPIPE_
// equivalent and per retired setting that is set, for the caller to log. Apply
// does not log itself, because it must run before the logger is configured.
func Apply() []string {
	warnings := deprecationWarnings()
	for _, kv := range os.Environ() {
		name, value, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasPrefix(name, Prefix) {
			continue
		}
		for _, legacy := range legacyNamesFor(name) {
			// Only fails on a malformed name, which cannot occur for a name
			// that came from the environment in the first place.
			_ = os.Setenv(legacy, value)
		}
	}
	return warnings
}

// legacyNamesFor returns the legacy variables a GIGAPIPE_ name feeds.
func legacyNamesFor(name string) []string {
	if names, ok := irregular[name]; ok {
		return names
	}
	return []string{"QRYN_" + strings.TrimPrefix(name, Prefix)}
}

// deprecationWarnings reports legacy variables that are set while their
// GIGAPIPE_ equivalent is not, so an operator is told which name to move to,
// and retired settings that are set, with what takes their place.
// It runs before Apply rewrites anything, so it sees what the deployment
// actually set.
func deprecationWarnings() []string {
	var out []string
	for legacy, r := range deprecated {
		if _, set := os.LookupEnv(legacy); !set {
			continue
		}
		if r.retired != "" {
			out = append(out, fmt.Sprintf("%s has no effect; %s", legacy, r.retired))
			continue
		}
		if _, set := os.LookupEnv(r.name); set {
			continue
		}
		out = append(out, fmt.Sprintf(
			"%s is deprecated and will be removed in a future release; use %s instead",
			legacy, r.name))
	}
	sort.Strings(out)
	return out
}
