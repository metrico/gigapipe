package metricread

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

func matcher(t labels.MatchType, name, value string) *labels.Matcher {
	return labels.MustNewMatcher(t, name, value)
}

func TestSelectorPredicate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		selectors [][]*labels.Matcher
		want      string
	}{
		{"name equal", [][]*labels.Matcher{{matcher(labels.MatchEqual, "__name__", "up")}},
			"name = 'up'"},
		{"name not equal", [][]*labels.Matcher{{matcher(labels.MatchNotEqual, "__name__", "up")}},
			"name != 'up'"},
		{"name regex", [][]*labels.Matcher{{matcher(labels.MatchRegexp, "__name__", "up|down")}},
			"match(name, '^(?:up|down)$')"},
		{"name not regex", [][]*labels.Matcher{{matcher(labels.MatchNotRegexp, "__name__", "up.*")}},
			"NOT match(name, '^(?:up.*)$')"},
		{"label equal", [][]*labels.Matcher{{matcher(labels.MatchEqual, "job", "api")}},
			"labels['job'] = 'api'"},
		{"label not equal", [][]*labels.Matcher{{matcher(labels.MatchNotEqual, "job", "api")}},
			"labels['job'] != 'api'"},
		{"label regex", [][]*labels.Matcher{{matcher(labels.MatchRegexp, "code", `5\d\d`)}},
			`match(labels['code'], '^(?:5\\d\\d)$')`},
		{"label not regex", [][]*labels.Matcher{{matcher(labels.MatchNotRegexp, "code", "2..")}},
			"NOT match(labels['code'], '^(?:2..)$')"},
		{"missing label reads empty", [][]*labels.Matcher{{matcher(labels.MatchEqual, "env", "")}},
			"labels['env'] = ''"},
		{"quotes escaped", [][]*labels.Matcher{{matcher(labels.MatchEqual, "path", "it's")}},
			`labels['path'] = 'it\'s'`},
		{"matchers of one selector ANDed",
			[][]*labels.Matcher{{matcher(labels.MatchEqual, "__name__", "up"), matcher(labels.MatchNotEqual, "job", "")}},
			"name = 'up' AND labels['job'] != ''"},
		{"selectors ORed",
			[][]*labels.Matcher{
				{matcher(labels.MatchEqual, "__name__", "up"), matcher(labels.MatchEqual, "job", "a")},
				{matcher(labels.MatchRegexp, "__name__", "down")},
			},
			"(name = 'up' AND labels['job'] = 'a') OR (match(name, '^(?:down)$'))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SelectorPredicate(tc.selectors...); got != tc.want {
				t.Fatalf("got  %s\nwant %s", got, tc.want)
			}
		})
	}
}
