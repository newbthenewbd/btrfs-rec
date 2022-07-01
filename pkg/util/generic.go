package util

import (
	"sort"

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

func RemoveAllFromSlice[T comparable](haystack []T, needle T) []T {
	for i, straw := range haystack {
		if needle == straw {
			return append(
				haystack[:i],
				RemoveAllFromSlice(haystack[i+1:], needle)...)
		}
	}
	return haystack
}

func RemoveAllFromSliceFunc[T any](haystack []T, f func(T) bool) []T {
	for i, straw := range haystack {
		if f(straw) {
			return append(
				haystack[:i],
				RemoveAllFromSliceFunc(haystack[i+1:], f)...)
		}
	}
	return haystack
}

func ReverseSlice[T any](slice []T) {
	for i := 0; i < len(slice)/2; i++ {
		j := (len(slice) - 1) - i
		slice[i], slice[j] = slice[j], slice[i]
	}
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

func MapKeys[K comparable, V any](m map[K]V) []K {
	ret := make([]K, 0, len(m))
	for k := range m {
		ret = append(ret, k)
	}
	return ret
}

func SortSlice[T constraints.Ordered](slice []T) {
	sort.Slice(slice, func(i, j int) bool {
		return slice[i] < slice[j]
	})
}

func SortedMapKeys[K constraints.Ordered, V any](m map[K]V) []K {
	ret := MapKeys(m)
	SortSlice(ret)
	return ret
}
