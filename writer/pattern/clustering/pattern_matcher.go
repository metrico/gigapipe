package clustering

import (
	"strings"
)

type patternMatcherV1 struct {
	parts         []string
	unknownPrefix bool
	unknownSuffix bool
}

func newPatternMatcherV1(pattern []Token) patternMatcherV1 {
	res := patternMatcherV1{
		parts: make([]string, 1, len(pattern)),
	}
	for i, t := range pattern {
		if t.Type == Generalized {
			res.unknownPrefix = res.unknownPrefix || (i == 0)
			res.unknownSuffix = res.unknownSuffix || (i == len(pattern)-1)
			if res.parts[len(res.parts)-1] != "" {
				res.parts = append(res.parts, "")
			}
			continue
		}
		res.parts[len(res.parts)-1] += t.Value
	}
	return res
}

func (m patternMatcherV1) match(s string) bool {
	var start int
	var end int
	for i, part := range m.parts {
		start = strings.Index(s, part)
		end = start + len(part)
		if start == -1 {
			return false
		}
		if i == 0 && !m.unknownPrefix && start != 0 {
			return false
		}
		s = s[end:]
	}
	return m.unknownSuffix || s == ""
}
