// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

type lruEntry[K comparable, V any] struct {
	key K
	val V
}

// lruCache is a NON-thread-safe cache with Least Recently Used
// eviction.
//
// lruCache is non-thread-safe and unexported because it only exists
// for internal use by the more sophisticated ARCache.
//
// Similarly, it does not implement some common features of an LRU
// cache, such as specifying a maximum size (instead requiring the
// `.EvictOldest` method to be called), because it is only meant for
// use by the ARCache; which does not need such features.
type lruCache[K comparable, V any] struct {
	// OnRemove is (if non-nil) called *after* removal whenever
	// an entry is removed, for any reason:
	//
	//  - evicted by .EvictOldest()
	//  - replaced by .Store(k, v)
	//  - deleted by .Delete(k)
	OnRemove func(K, V)
	// OnEvict is (if non-nil) called *after* removal whenever an
	// entry is evicted by .EvictOldest().  If both OnEvict and
	// OnRemove are set, OnRemove is called first.
	OnEvict func(K, V)

	byAge  LinkedList[lruEntry[K, V]]
	byName map[K]*LinkedListEntry[lruEntry[K, V]]
}

var _ Map[int, string] = (*lruCache[int, string])(nil)

func (c *lruCache[K, V]) rem(entry *LinkedListEntry[lruEntry[K, V]]) {
	k, v := entry.Value.key, entry.Value.val
	delete(c.byName, entry.Value.key)
	c.byAge.Delete(entry)
	if c.OnRemove != nil {
		c.OnRemove(k, v)
	}
}

func (c *lruCache[K, V]) evict(entry *LinkedListEntry[lruEntry[K, V]]) {
	k, v := entry.Value.key, entry.Value.val
	c.rem(entry)
	if c.OnEvict != nil {
		c.OnEvict(k, v)
	}
}

// EvictOldest deletes the oldest entry in the cache.
//
// It is a panic to call EvictOldest if the cache is empty.
func (c *lruCache[K, V]) EvictOldest() {
	c.evict(c.byAge.Oldest())
}

// Store a key/value pair in to the cache.
func (c *lruCache[K, V]) Store(k K, v V) {
	if c.byName == nil {
		c.byName = make(map[K]*LinkedListEntry[lruEntry[K, V]])
	} else if old, ok := c.byName[k]; ok {
		c.rem(old)
	}
	c.byName[k] = c.byAge.Store(lruEntry[K, V]{key: k, val: v})
}

// Load an entry from the cache, recording a "use" for the purposes of
// "least-recently-used" eviction.
func (c *lruCache[K, V]) Load(k K) (v V, ok bool) {
	entry, ok := c.byName[k]
	if !ok {
		var zero V
		return zero, false
	}
	c.byAge.MoveToNewest(entry)
	return entry.Value.val, true
}

// Peek is like Load, but doesn't count as a "use" for
// "least-recently-used".
func (c *lruCache[K, V]) Peek(k K) (v V, ok bool) {
	entry, ok := c.byName[k]
	if !ok {
		var zero V
		return zero, false
	}
	return entry.Value.val, true
}

// Has returns whether an entry is present in the cache.  Calling Has
// does not count as a "use" for "least-recently-used".
func (c *lruCache[K, V]) Has(k K) bool {
	_, ok := c.byName[k]
	return ok
}

// Delete an entry from the cache.
func (c *lruCache[K, V]) Delete(k K) {
	if entry, ok := c.byName[k]; ok {
		c.rem(entry)
	}
}

// Len returns the number of entries in the cache.
func (c *lruCache[K, V]) Len() int {
	return len(c.byName)
}
