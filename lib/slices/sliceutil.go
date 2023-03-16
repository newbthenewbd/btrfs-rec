// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package slices implements generic (type-parameterized) utilities
// for working with simple Go slices.
package slices

import (
	"sort"

	"golang.org/x/exp/constraints"
)

func Contains[T comparable](needle T, haystack []T) bool {
	for _, straw := range haystack {
		if needle == straw {
			return true
		}
	}
	return false
}

func RemoveAll[T comparable](haystack []T, needle T) []T {
	for i, straw := range haystack {
		if needle == straw {
			return append(
				haystack[:i],
				RemoveAll(haystack[i+1:], needle)...)
		}
	}
	return haystack
}

func RemoveAllFunc[T any](haystack []T, f func(T) bool) []T {
	for i, straw := range haystack {
		if f(straw) {
			return append(
				haystack[:i],
				RemoveAllFunc(haystack[i+1:], f)...)
		}
	}
	return haystack
}

func Reverse[T any](slice []T) {
	for i := 0; i < len(slice)/2; i++ {
		j := (len(slice) - 1) - i
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func Max[T constraints.Ordered](a T, rest ...T) T {
	ret := a
	for _, b := range rest {
		if b > a {
			ret = b
		}
	}
	return ret
}

func Min[T constraints.Ordered](a T, rest ...T) T {
	ret := a
	for _, b := range rest {
		if b < a {
			ret = b
		}
	}
	return ret
}

func Sort[T constraints.Ordered](slice []T) {
	sort.Slice(slice, func(i, j int) bool {
		return slice[i] < slice[j]
	})
}

// returns (a+b)/2, but avoids overflow
func avg(a, b int) int {
	return int(uint(a+b) >> 1)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Search the slice for a value for which `fn(slice[i]) = 0`.
//
//	: + + + 0 0 0 - - -
//	:       ^ ^ ^
//	:       any of
//
// You can conceptualize `fn` as subtraction:
//
//	func(straw T) int {
//	    return needle - straw
//	}
func Search[T any](slice []T, fn func(T) int) (int, bool) {
	beg, end := 0, len(slice)
	for beg < end {
		midpoint := avg(beg, end)
		direction := fn(slice[midpoint])
		switch {
		case direction < 0:
			end = midpoint
		case direction > 0:
			beg = midpoint + 1
		case direction == 0:
			return midpoint, true
		}
	}
	return 0, false
}

// Search the slice for the left-most value for which `fn(slice[i]) = 0`.
//
//	: + + + 0 0 0 - - -
//	:       ^
//
// You can conceptualize `fn` as subtraction:
//
//	func(straw T) int {
//	    return needle - straw
//	}
func SearchLowest[T any](slice []T, fn func(T) int) (int, bool) {
	lastBad, firstGood, firstBad := -1, len(slice), len(slice)
	for lastBad+1 < min(firstGood, firstBad) {
		midpoint := avg(lastBad, min(firstGood, firstBad))
		direction := fn(slice[midpoint])
		switch {
		case direction < 0:
			firstBad = midpoint
		case direction > 0:
			lastBad = midpoint
		default:
			firstGood = midpoint
		}
	}
	if firstGood == len(slice) {
		return 0, false
	}
	return firstGood, true
}

// Search the slice for the right-most value for which `fn(slice[i]) = 0`.
//
//	: + + + 0 0 0 - - -
//	:           ^
//
// You can conceptualize `fn` as subtraction:
//
//	func(straw T) int {
//	    return needle - straw
//	}
func SearchHighest[T any](slice []T, fn func(T) int) (int, bool) {
	lastBad, lastGood, firstBad := -1, -1, len(slice)
	for max(lastBad, lastGood)+1 < firstBad {
		midpoint := avg(max(lastBad, lastGood), firstBad)
		direction := fn(slice[midpoint])
		switch {
		case direction < 0:
			firstBad = midpoint
		case direction > 0:
			lastBad = midpoint
		default:
			lastGood = midpoint
		}
	}
	if lastGood < 0 {
		return 0, false
	}
	return lastGood, true
}
