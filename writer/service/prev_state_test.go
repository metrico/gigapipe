package service

import (
	"fmt"
	"testing"
)

func TestArrayRevert(t *testing.T) {
	a := []int{1}
	b := a
	// a = append(a, 1)
	a = b
	fmt.Println(a)
}
