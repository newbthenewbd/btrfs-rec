// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	lru "github.com/hashicorp/golang-lru"
)

// LRUCache is a least-recently-used(ish) cache.  A zero LRUCache is
// not usable; it must be initialized with NewLRUCache.
type LRUCache[K comparable, V any] struct {
	inner *lru.ARCCache
}

func NewLRUCache[K comparable, V any](size int) *LRUCache[K, V] {
	c := new(LRUCache[K, V])
	c.inner, _ = lru.NewARC(size)
	return c
}

func (c *LRUCache[K, V]) Add(key K, value V) {
	c.inner.Add(key, value)
}

func (c *LRUCache[K, V]) Contains(key K) bool {
	return c.inner.Contains(key)
}

func (c *LRUCache[K, V]) Get(key K) (value V, ok bool) {
	_value, ok := c.inner.Get(key)
	if ok {
		//nolint:forcetypeassert // Typed wrapper around untyped lib.
		value = _value.(V)
	}
	return value, ok
}

func (c *LRUCache[K, V]) Keys() []K {
	untyped := c.inner.Keys()
	typed := make([]K, len(untyped))
	for i := range untyped {
		//nolint:forcetypeassert // Typed wrapper around untyped lib.
		typed[i] = untyped[i].(K)
	}
	return typed
}

func (c *LRUCache[K, V]) Len() int {
	return c.inner.Len()
}

func (c *LRUCache[K, V]) Peek(key K) (value V, ok bool) {
	_value, ok := c.inner.Peek(key)
	if ok {
		//nolint:forcetypeassert // Typed wrapper around untyped lib.
		value = _value.(V)
	}
	return value, ok
}

func (c *LRUCache[K, V]) Purge() {
	c.inner.Purge()
}

func (c *LRUCache[K, V]) Remove(key K) {
	c.inner.Remove(key)
}

func (c *LRUCache[K, V]) GetOrElse(key K, fn func() V) V {
	var value V
	var ok bool
	for value, ok = c.Get(key); !ok; value, ok = c.Get(key) {
		c.Add(key, fn())
	}
	return value
}
