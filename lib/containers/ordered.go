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

// An Ordered is a type that has a
//
//	func (a T) Compare(b T) int
//
// method that returns a value <1 if a is "less than" b, >1 if a is
// "greater than" b, or 0 if a is "equal to" b.
//
// You can conceptualize as subtraction:
//
//	func (a T) Compare(b T) int {
//		return a - b
//	}
//
// Be careful to avoid integer overflow if actually implementing it as
// subtraction.
type Ordered[T _Ordered[T]] _Ordered[T]

// NativeOrdered takes a type that is natively-ordered (integer types,
// float types, and string types), and wraps them such that they
// implement the Ordered interface.
type NativeOrdered[T constraints.Ordered] struct {
	Val T
}

// NativeCompare implements the Ordered[T] Compare operation for
// natively-ordered (integer types, float types, and string types).
// While this operation be conceptualized as subtration, NativeCompare
// is careful to avoid integer overflow.
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

// Compare implements Ordered[T].
func (a NativeOrdered[T]) Compare(b NativeOrdered[T]) int {
	return NativeCompare(a.Val, b.Val)
}

var _ Ordered[NativeOrdered[int]] = NativeOrdered[int]{}
