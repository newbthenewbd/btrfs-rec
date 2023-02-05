// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"golang.org/x/exp/constraints"
)

type _Ordered[T any] interface {
	Compare(T) int
}

type Ordered[T _Ordered[T]] _Ordered[T]

type NativeOrdered[T constraints.Ordered] struct {
	Val T
}

func NativeCompare[T constraints.Ordered](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func (a NativeOrdered[T]) Compare(b NativeOrdered[T]) int {
	return NativeCompare(a.Val, b.Val)
}

var _ Ordered[NativeOrdered[int]] = NativeOrdered[int]{}
