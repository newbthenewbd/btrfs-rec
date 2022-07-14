// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"golang.org/x/exp/constraints"
)

type Ordered[T interface{ Cmp(T) int }] interface {
	Cmp(T) int
}

type NativeOrdered[T constraints.Ordered] struct {
	Val T
}

func NativeCmp[T constraints.Ordered](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func (a NativeOrdered[T]) Cmp(b NativeOrdered[T]) int {
	return NativeCmp(a.Val, b.Val)
}

var _ Ordered[NativeOrdered[int]] = NativeOrdered[int]{}
