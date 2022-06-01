package btrfs

import (
	"golang.org/x/exp/constraints"
)

func inSlice[T comparable](needle T, haystack []T) bool {
	for _, straw := range haystack {
		if needle == straw {
			return true
		}
	}
	return false
}

func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}
