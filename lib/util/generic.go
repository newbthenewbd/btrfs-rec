// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package util

import (
	"sort"
	"sync"

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

type SyncMap[K comparable, V any] struct {
	inner sync.Map
}

func (m *SyncMap[K, V]) Delete(key K) { m.inner.Delete(key) }
func (m *SyncMap[K, V]) Load(key K) (value V, ok bool) {
	_value, ok := m.inner.Load(key)
	if ok {
		value = _value.(V)
	}
	return value, ok
}
func (m *SyncMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	_value, ok := m.inner.LoadAndDelete(key)
	if ok {
		value = _value.(V)
	}
	return value, ok
}
func (m *SyncMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	_actual, loaded := m.inner.LoadOrStore(key, value)
	actual = _actual.(V)
	return actual, loaded
}
func (m *SyncMap[K, V]) Range(f func(key K, value V) bool) {
	m.inner.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}
func (m *SyncMap[K, V]) Store(key K, value V) { m.inner.Store(key, value) }

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
