// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package maps implements generic (type-parameterized) utilities for
// working with simple Go maps.
package maps

import (
	"golang.org/x/exp/constraints"

	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

func Keys[K comparable, V any](m map[K]V) []K {
	ret := make([]K, 0, len(m))
	for k := range m {
		ret = append(ret, k)
	}
	return ret
}

func SortedKeys[K constraints.Ordered, V any](m map[K]V) []K {
	ret := Keys(m)
	slices.Sort(ret)
	return ret
}
