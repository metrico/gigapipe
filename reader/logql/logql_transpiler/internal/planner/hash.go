package planner

import (
	"sort"
	"strings"

	"github.com/go-faster/city"
)

// fingerprint computes a deterministic hash of a label set.
// Keys are sorted alphabetically and joined as "k=v,k=v,...".
// The SQL equivalent is:
//
//	cityHash64(arrayStringConcat(arrayMap((k,v)->concat(k,'=',v),mapKeys(labels),mapValues(labels)),','))
//
// ClickHouse Map keys are always stored in sorted order, so mapKeys() returns
// the same sorted sequence as sort.Strings here, making the two formulas produce
// identical results for the same label set.
func fingerprint(labels map[string]string) uint64 {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + labels[k]
	}
	return city.CH64([]byte(strings.Join(parts, ",")))
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
