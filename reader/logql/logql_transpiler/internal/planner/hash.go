package planner

import (
	"unsafe"

	"github.com/go-faster/city"
)

func fingerprint(labels map[string]string) uint64 {
	descr := [3]uint64{0, 0, 1}
	for k, v := range labels {
		a := k + v
		descr[0] += city.CH64([]byte(a))
		descr[1] ^= city.CH64([]byte(a))
		descr[2] *= 1779033703 + 2*city.CH64([]byte(a))

	}
	return city.CH64(unsafe.Slice((*byte)(unsafe.Pointer(&descr[0])), 24))
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
