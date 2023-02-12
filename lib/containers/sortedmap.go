// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

type orderedKV[K Ordered[K], V any] struct {
	K K
	V V
}

func (a orderedKV[K, V]) Compare(b orderedKV[K, V]) int {
	return a.K.Compare(b.K)
}

type SortedMap[K Ordered[K], V any] struct {
	inner RBTree[orderedKV[K, V]]
}

func (m *SortedMap[K, V]) Delete(key K) {
	m.inner.Delete(m.inner.Search(func(kv orderedKV[K, V]) int {
		return key.Compare(kv.K)
	}))
}

func (m *SortedMap[K, V]) Load(key K) (value V, ok bool) {
	node := m.inner.Search(func(kv orderedKV[K, V]) int {
		return key.Compare(kv.K)
	})
	if node == nil {
		var zero V
		return zero, false
	}
	return node.Value.V, true
}

func (m *SortedMap[K, V]) Store(key K, value V) {
	m.inner.Insert(orderedKV[K, V]{
		K: key,
		V: value,
	})
}

func (m *SortedMap[K, V]) Range(fn func(key K, value V) bool) {
	m.inner.Range(func(node *RBNode[orderedKV[K, V]]) bool {
		return fn(node.Value.K, node.Value.V)
	})
}

func (m *SortedMap[K, V]) Subrange(rangeFn func(K, V) int, handleFn func(K, V) bool) {
	m.inner.Subrange(
		func(kv orderedKV[K, V]) int { return rangeFn(kv.K, kv.V) },
		func(node *RBNode[orderedKV[K, V]]) bool { return handleFn(node.Value.K, node.Value.V) })
}

func (m *SortedMap[K, V]) Search(fn func(K, V) int) (K, V, bool) {
	node := m.inner.Search(func(kv orderedKV[K, V]) int {
		return fn(kv.K, kv.V)
	})
	if node == nil {
		var zeroK K
		var zeroV V
		return zeroK, zeroV, false
	}
	return node.Value.K, node.Value.V, true
}

func (m *SortedMap[K, V]) Len() int {
	return m.inner.Len()
}
