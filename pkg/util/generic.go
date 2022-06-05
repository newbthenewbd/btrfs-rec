package util

import (
	"golang.org/x/exp/constraints"
)

func InSlice[T comparable](needle T, haystack []T) bool {
	for _, straw := range haystack {
		if needle == straw {
			return true
		}
	}
	return false
}

func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}
