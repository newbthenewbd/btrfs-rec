// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

type Map[K comparable, V any] interface {
	Store(K, V)
	Load(K) (V, bool)
	Has(K) bool
	Delete(K)
	Len() int
}

type RangeMap[K comparable, V any] interface {
	Map[K, V]
	Range(func(K, V) bool)
}

type SubrangeMap[K comparable, V any] interface {
	RangeMap[K, V]
	Subrange(rangeFn func(K, V) int, handleFn func(K, V) bool)
}

func LoadOrElse[K comparable, V any](m Map[K, V], k K, vFn func(K) V) V {
	if v, ok := m.Load(k); ok {
		return v
	}
	v := vFn(k)
	m.Store(k, v)
	return v
}
