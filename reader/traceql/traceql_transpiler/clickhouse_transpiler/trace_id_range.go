package clickhouse_transpiler

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"strings"
)

// idRange is the trace ids in [lo, hi), or from lo on when hi is nil.
type idRange struct {
	lo [16]byte
	hi *[16]byte
}

// traceIDPortion is portion i of n of the 16-byte trace ids; the n portions
// partition every id.
func traceIDPortion(n, i int) []idRange {
	// Ids with a nonzero high half: high half from 1 + k(2^64-1)/n.
	high := func(k int) [16]byte {
		wordHi, wordLo := bits.Mul64(uint64(k), math.MaxUint64)
		q, _ := bits.Div64(wordHi, wordLo, uint64(n))
		var b [16]byte
		binary.BigEndian.PutUint64(b[:8], q+1)
		return b
	}
	// Ids with a zero high half: low half from k 2^64/n.
	low := func(k int) [16]byte {
		q, _ := bits.Div64(uint64(k), 0, uint64(n))
		var b [16]byte
		binary.BigEndian.PutUint64(b[8:], q)
		return b
	}
	res := []idRange{{lo: high(i)}, {lo: low(i)}}
	if i < n-1 {
		h, l := high(i+1), low(i+1)
		res[0].hi, res[1].hi = &h, &l
	} else {
		h := high(0)
		res[1].hi = &h
	}
	return res
}

// traceIDPortionSQL is the condition on trace_id of traceIDPortion(n, i).
func traceIDPortionSQL(n, i int) string {
	var ranges []string
	for _, r := range traceIDPortion(n, i) {
		var conds []string
		if r.lo != ([16]byte{}) {
			conds = append(conds, fmt.Sprintf("trace_id >= unhex('%x')", r.lo))
		}
		if r.hi != nil {
			conds = append(conds, fmt.Sprintf("trace_id < unhex('%x')", *r.hi))
		}
		ranges = append(ranges, strings.Join(conds, " AND "))
	}
	return strings.Join(ranges, " OR ")
}
