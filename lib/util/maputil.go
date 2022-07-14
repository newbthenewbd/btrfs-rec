// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package util

import (
	"golang.org/x/exp/constraints"
)

func MapKeys[K comparable, V any](m map[K]V) []K {
	ret := make([]K, 0, len(m))
	for k := range m {
		ret = append(ret, k)
	}
	return ret
}

func SortedMapKeys[K constraints.Ordered, V any](m map[K]V) []K {
	ret := MapKeys(m)
	SortSlice(ret)
	return ret
}
