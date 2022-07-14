// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"sync"

	lru "github.com/hashicorp/golang-lru"
)

// LRUCache is a least-recently-used(ish) cache.  A zero LRUCache is
// usable and has a cache size of 128 items; use NewLRUCache to set a
// different size.
type LRUCache[K comparable, V any] struct {
	initOnce sync.Once
	inner    *lru.ARCCache
}

func NewLRUCache[K comparable, V any](size int) *LRUCache[K, V] {
	c := new(LRUCache[K, V])
	c.initOnce.Do(func() {
		c.inner, _ = lru.NewARC(size)
	})
	return c
}

func (c *LRUCache[K, V]) init() {
	c.initOnce.Do(func() {
		c.inner, _ = lru.NewARC(128)
	})
}

func (c *LRUCache[K, V]) Add(key K, value V) {
	c.init()
	c.inner.Add(key, value)
}
func (c *LRUCache[K, V]) Contains(key K) bool {
	c.init()
	return c.inner.Contains(key)
}
func (c *LRUCache[K, V]) Get(key K) (value V, ok bool) {
	c.init()
	_value, ok := c.inner.Get(key)
	if ok {
		value = _value.(V)
	}
	return value, ok
}
func (c *LRUCache[K, V]) Keys() []K {
	c.init()
	untyped := c.inner.Keys()
	typed := make([]K, len(untyped))
	for i := range untyped {
		typed[i] = untyped[i].(K)
	}
	return typed
}
func (c *LRUCache[K, V]) Len() int {
	c.init()
	return c.inner.Len()
}
func (c *LRUCache[K, V]) Peek(key K) (value V, ok bool) {
	c.init()
	_value, ok := c.inner.Peek(key)
	if ok {
		value = _value.(V)
	}
	return value, ok
}
func (c *LRUCache[K, V]) Purge() {
	c.init()
	c.inner.Purge()
}
func (c *LRUCache[K, V]) Remove(key K) {
	c.init()
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
