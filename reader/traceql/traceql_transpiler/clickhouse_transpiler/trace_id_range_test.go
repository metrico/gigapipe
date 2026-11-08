package clickhouse_transpiler

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"testing"
)

// boundaryIDs are trace ids at the edges of the id space and of its halves.
var boundaryIDs = []string{
	"00000000000000000000000000000000", "00000000000000000000000000000001",
	"0000000000000000ffffffffffffffff", "00000000000000010000000000000000",
	"00000000000000010000000000000001", "00000000000000000000000000000abc",
	"7fffffffffffffffffffffffffffffff", "80000000000000000000000000000000",
	"55555555555555550000000000000000", "55555555555555560000000000000000",
	"00000000000000005555555555555555", "00000000000000005555555555555554",
	"aaaaaaaaaaaaaaab0000000000000000", "aaaaaaaaaaaaaaaaffffffffffffffff",
	"ffffffffffffffff0000000000000000", "ffffffffffffffffffffffffffffffff",
	"0123456789abcdef0000000000000000",
}

func inRange(id []byte, r idRange) bool {
	return bytes.Compare(id, r.lo[:]) >= 0 && (r.hi == nil || bytes.Compare(id, r.hi[:]) < 0)
}

// Each trace id, of either half, falls in exactly one of the n portions.
func TestTraceIDPortionsPartitionEveryID(t *testing.T) {
	ids := make([][]byte, 0, len(boundaryIDs)+2000)
	for _, s := range boundaryIDs {
		b, _ := hex.DecodeString(s)
		ids = append(ids, b)
	}
	rnd := rand.New(rand.NewSource(1))
	for range 1000 {
		var full, short [16]byte
		rnd.Read(full[:])
		rnd.Read(short[8:])
		ids = append(ids, full[:], short[:])
	}
	for _, n := range []int{1, 2, 3, 5, 7, 64, 1000} {
		perPortion := make([]int, n)
		for _, id := range ids {
			var in []int
			for i := range n {
				for _, r := range traceIDPortion(n, i) {
					if inRange(id, r) {
						in = append(in, i)
					}
				}
			}
			if len(in) != 1 {
				t.Fatalf("%d portions: %x in portions %v", n, id, in)
			}
			perPortion[in[0]]++
		}
		if n <= 7 {
			for i, c := range perPortion {
				if c < len(ids)/n/2 {
					t.Errorf("%d portions: portion %d holds %d of %d ids", n, i, c, len(ids))
				}
			}
		}
	}
}
