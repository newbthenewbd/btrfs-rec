// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"sync"
)

type SyncMap[K comparable, V any] struct {
	inner sync.Map
}

func (m *SyncMap[K, V]) Delete(key K) {
	m.inner.Delete(key)
}
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
func (m *SyncMap[K, V]) Store(key K, value V) {
	m.inner.Store(key, value)
}
