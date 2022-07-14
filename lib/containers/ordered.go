// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"golang.org/x/exp/constraints"
)

func CmpUint[T constraints.Unsigned](a, b T) int {
	switch {
	case a < b:
		return -1
	case a == b:
		return 0
	default:
		return 1
	}
}

type Ordered[T interface{ Cmp(T) int }] interface {
	Cmp(T) int
}

type NativeOrdered[T constraints.Ordered] struct {
	Val T
}

func (a NativeOrdered[T]) Cmp(b NativeOrdered[T]) int {
	switch {
	case a.Val < b.Val:
		return -1
	case a.Val > b.Val:
		return 1
	default:
		return 0
	}
}

var _ Ordered[NativeOrdered[int]] = NativeOrdered[int]{}
