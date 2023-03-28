// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package caching

import (
	"context"
	"fmt"
	"sync"
)

// NewLRUCache returns a new thread-safe Cache with a simple
// Least-Recently-Used eviction policy.
//
// It is invalid (runtime-panic) to call NewLRUCache with a
// non-positive capacity or a nil source.
func NewLRUCache[K comparable, V any](cap int, src Source[K, V]) Cache[K, V] {
	if cap <= 0 {
		panic(fmt.Errorf("caching.NewLRUCache: invalid capacity: %v", cap))
	}
	if src == nil {
		panic(fmt.Errorf("caching.NewLRUCache: nil source"))
	}
	ret := &lruCache[K, V]{
		cap: cap,
		src: src,
	}
	for i := 0; i < cap; i++ {
		ret.unused.Store(new(LinkedListEntry[lruEntry[K, V]]))
	}
	return ret
}

type lruEntry[K comparable, V any] struct {
	key K
	val V

	refs int
	del  chan struct{} // non-nil if a delete is waiting on .refs to drop to zero
}

type lruCache[K comparable, V any] struct {
	cap int
	src Source[K, V]

	mu sync.Mutex

	// Pinned entries are in .byName, but not in any LinkedList.
	unused    LinkedList[lruEntry[K, V]]
	evictable LinkedList[lruEntry[K, V]] // only entries with .refs==0
	byName    map[K]*LinkedListEntry[lruEntry[K, V]]

	waiters LinkedList[chan struct{}]
}

// Blocking primitives /////////////////////////////////////////////////////////

// Because of pinning, there might not actually be an available entry
// for us to use/evict.  If we need an entry to use or evict, we'll
// call waitForAvail to block until there is en entry that is either
// unused or evictable.  We'll give waiters FIFO priority.
func (c *lruCache[K, V]) waitForAvail() {
	if !(c.unused.IsEmpty() && c.evictable.IsEmpty()) {
		return
	}
	ch := make(chan struct{})
	c.waiters.Store(&LinkedListEntry[chan struct{}]{Value: ch})
	c.mu.Unlock()
	<-ch
	c.mu.Lock()
}

// notifyAvail is called when an entry becomes unused or evictable,
// and wakes up the highest-priority .waitForAvail() waiter (if there
// is one).
func (c *lruCache[K, V]) notifyAvail(entry *LinkedListEntry[lruEntry[K, V]]) {
	waiter := c.waiters.Oldest
	if waiter == nil {
		return
	}
	c.waiters.Delete(waiter)
	close(waiter.Value)
}

// Calling .Delete(k) on an entry that is pinned needs to block until
// the entry is no longer pinned.
func (c *lruCache[K, V]) unlockAndWaitForDel(entry *LinkedListEntry[lruEntry[K, V]]) {
	if entry.Value.del == nil {
		entry.Value.del = make(chan struct{})
	}
	ch := entry.Value.del
	c.mu.Unlock()
	<-ch
}

// notifyOfDel unblocks any calls to .Delete(k), notifying them that
// the entry has been deleted and they can now return.
func (c *lruCache[K, V]) notifyOfDel(entry *LinkedListEntry[arcLiveEntry[K, V]]) {
	if entry.Value.del != nil {
		close(entry.Value.del)
		entry.Value.del = nil
	}
}

// Main implementation /////////////////////////////////////////////////////////

// lruReplace is the LRU(c) replacement policy.  It returns an entry
// that is not in any list.
func (c *lruCache[K, V]) lruReplace() *LinkedListEntry[lruEntry[K, V]] {
	c.waitForAvail()

	// If the cache isn't full, no need to do an eviction.
	if entry := c.unused.Oldest; entry != nil {
		c.unused.Delete(entry)
		return entry
	}

	// Replace the oldest entry.
	entry := c.evictable.Oldest
	c.evictable.Delete(entry)
	delete(c.byName, entry.Value.key)
	return entry
}

// Acquire implements the 'Cache' interface.
func (c *lruCache[K, V]) Acquire(ctx context.Context, k K) *V {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byName == nil {
		c.byName = make(map[K]*LinkedListEntry[lruEntry[K, V]], c.cap)
	}

	entry := c.byName[k]
	if entry != nil {
		if entry.Value.refs == 0 {
			c.evictable.Delete(entry)
		}
		entry.Value.refs++
	} else {
		entry = c.lruReplace()

		entry.Value.key = k
		c.src.Load(ctx, k, &entry.Value.val)
		entry.Value.refs = 1

		c.byName[k] = entry
	}

	return &entry.Value.val
}

// Delete implements the 'Cache' interface.
func (c *lruCache[K, V]) Delete(k K) {
	c.mu.Lock()

	entry := c.byName[k]
	if entry == nil {
		return
	}
	if entry.Value.refs > 0 {
		// Let .Release(k) do the deletion when the
		// refcount drops to 0.
		c.unlockAndWaitForDel(entry)
		return
	}
	delete(c.byName, k)
	c.evictable.Delete(entry)
	c.unused.Store(entry)

	// No need to call c.notifyAvail(); if we were able to delete
	// it, it was already available.

	c.mu.Unlock()
}

// Release implements the 'Cache' interface.
func (c *lruCache[K, V]) Release(k K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := c.byName[k]
	if entry == nil || entry.Value.refs <= 0 {
		panic(fmt.Errorf("caching.lruCache.Release called on key that is not held: %v", k))
	}

	entry.Value.refs--
	if entry.Value.refs == 0 {
		if entry.Value.del != nil {
			delete(c.byName, k)
			c.unused.Store(entry)
		} else {
			c.evictable.Store(entry)
		}
		c.notifyAvail(entry)
	}
}

// Flush implements the 'Cache' interface.
func (c *lruCache[K, V]) Flush(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, entry := range c.byName {
		c.src.Flush(ctx, &entry.Value.val)
	}
	for entry := c.unused.Oldest; entry != nil; entry = entry.Newer {
		c.src.Flush(ctx, &entry.Value.val)
	}
}
