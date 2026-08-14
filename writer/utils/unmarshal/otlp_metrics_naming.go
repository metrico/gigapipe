package unmarshal

import (
	"strings"
)

// unitWordMap maps UCUM unit codes to the Prometheus word equivalents used as
// metric name suffixes, per the OpenTelemetry Prometheus compatibility spec.
var unitWordMap = map[string]string{
	// Time
	"d":   "days",
	"h":   "hours",
	"min": "minutes",
	"s":   "seconds",
	"ms":  "milliseconds",
	"us":  "microseconds",
	"ns":  "nanoseconds",
	// Bytes
	"By":   "bytes",
	"KiBy": "kibibytes",
	"MiBy": "mebibytes",
	"GiBy": "gibibytes",
	"TiBy": "tibibytes",
	"KBy":  "kilobytes",
	"MBy":  "megabytes",
	"GBy":  "gigabytes",
	"TBy":  "terabytes",
	// SI
	"m": "meters",
	"V": "volts",
	"A": "amperes",
	"J": "joules",
	"W": "watts",
	"g": "grams",
	// Misc
	"Cel": "celsius",
	"Hz":  "hertz",
	"%":   "percent",
}

// perUnitWordMap maps UCUM rate denominators (the "s" in "By/s") to the word
// used in a "_per_<word>" metric name suffix.
var perUnitWordMap = map[string]string{
	"s":  "second",
	"m":  "minute",
	"h":  "hour",
	"d":  "day",
	"w":  "week",
	"mo": "month",
	"y":  "year",
}

// stripUnitAnnotations removes UCUM curly-brace annotations ("{packets}") from
// a unit string.
func stripUnitAnnotations(unit string) string {
	var b strings.Builder
	depth := 0
	for _, r := range unit {
		switch {
		case r == '{':
			depth++
		case r == '}':
			if depth > 0 {
				depth--
			}
		case depth == 0:
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

// unitWords converts a UCUM unit into (main, per) Prometheus suffix words:
// "By/s" -> ("bytes", "second"). Unknown units pass through only if they are
// already safe to embed in a metric name; otherwise they yield "".
func unitWords(unit string) (string, string) {
	unit = stripUnitAnnotations(unit)
	if unit == "" || unit == "1" {
		return "", ""
	}
	main, per := unit, ""
	if idx := strings.LastIndex(unit, "/"); idx >= 0 {
		main = strings.TrimSpace(unit[:idx])
		perRaw := strings.TrimSpace(unit[idx+1:])
		if w, ok := perUnitWordMap[perRaw]; ok {
			per = w
		} else if isWordSafe(perRaw) {
			per = perRaw
		}
	}
	if w, ok := unitWordMap[main]; ok {
		main = w
	} else if !isWordSafe(main) {
		main = ""
	}
	return main, per
}

// isWordSafe reports whether s can be embedded into a metric name verbatim.
func isWordSafe(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_'
		if !ok {
			return false
		}
	}
	return true
}

// normalizeMetricName maps an OTLP metric name onto the Prometheus name
// charset: invalid characters become "_", consecutive "_" runs collapse to
// one, and a leading digit gets a "_" prefix.
func normalizeMetricName(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	prevUnderscore := false
	for _, r := range name {
		valid := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == ':'
		if !valid {
			r = '_'
		}
		if r == '_' {
			if prevUnderscore {
				continue
			}
			prevUnderscore = true
		} else {
			prevUnderscore = false
		}
		b.WriteRune(r)
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "_"
	}
	if out[0] >= '0' && out[0] <= '9' {
		out = "_" + out
	}
	return out
}

// buildMetricName produces the stored Prometheus-style metric name from an
// OTLP metric name and unit: normalized charset, unit word suffixes, and
// "_total" for monotonic counters.
func buildMetricName(name, unit string, monotonicCounter bool) string {
	out := normalizeMetricName(name)
	mainUnit, perUnit := unitWords(unit)
	if mainUnit != "" && !hasNameToken(out, mainUnit) {
		out += "_" + mainUnit
	}
	if perUnit != "" && !strings.Contains(out, "_per_"+perUnit) {
		out += "_per_" + perUnit
	}
	if monotonicCounter && !strings.HasSuffix(out, "_total") {
		out += "_total"
	}
	return out
}

// hasNameToken reports whether name already contains the "_"-delimited token,
// so unit suffixes are not appended twice.
func hasNameToken(name, token string) bool {
	return name == token ||
		strings.HasSuffix(name, "_"+token) ||
		strings.Contains(name, "_"+token+"_") ||
		strings.HasPrefix(name, token+"_")
}
