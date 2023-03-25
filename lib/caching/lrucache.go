// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package caching

import (
	"context"
	"fmt"
	"sync"
)

type lruEntry[K comparable, V any] struct {
	key  K
	val  V
	refs int
	del  chan struct{}
}

type lruCache[K comparable, V any] struct {
	cap int
	src Source[K, V]

	mu sync.Mutex

	len int

	unused    LinkedList[lruEntry[K, V]]
	evictable LinkedList[lruEntry[K, V]] // only entries with .refs==0
	byName    map[K]*LinkedListEntry[lruEntry[K, V]]

	waiters LinkedList[chan *LinkedListEntry[lruEntry[K, V]]]
}

// NewLRUCache returns a new Cache with a simple Least-Recently-Used eviction
// policy.
//
// It is invalid (runtime-panic) to call NewLRUCache with a non-positive
// capacity or a nil source.
func NewLRUCache[K comparable, V any](cap int, src Source[K, V]) Cache[K, V] {
	if cap <= 0 {
		panic(fmt.Errorf("caching.NewLRUCache: invalid capacity: %v", cap))
	}
	if src == nil {
		panic(fmt.Errorf("caching.NewLRUCache: nil source"))
	}
	return &lruCache[K, V]{
		cap: cap,
		src: src,
	}
}

// Acquire implements Cache.
func (c *lruCache[K, V]) Acquire(ctx context.Context, k K) *V {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byName == nil {
		c.byName = make(map[K]*LinkedListEntry[lruEntry[K, V]], c.cap)
	}

	entry, ok := c.byName[k]
	if ok {
		if entry.Value.refs == 0 {
			c.evictable.Delete(entry)
		}
		entry.Value.refs++
	} else {
		switch {
		case !c.unused.IsEmpty():
			entry = c.unused.Oldest()
			c.unused.Delete(entry)
		case c.len < c.cap:
			entry = new(LinkedListEntry[lruEntry[K, V]])
			c.len++
		case !c.evictable.IsEmpty():
			entry = c.evictable.Oldest()
			c.evictable.Delete(entry)
			delete(c.byName, entry.Value.key)
		default:
			ch := make(chan *LinkedListEntry[lruEntry[K, V]])
			c.waiters.Store(&LinkedListEntry[chan *LinkedListEntry[lruEntry[K, V]]]{Value: ch})
			c.mu.Unlock()
			entry = <-ch
			c.mu.Lock()
		}

		entry.Value.key = k
		c.src.Load(ctx, k, &entry.Value.val)
		entry.Value.refs = 1

		c.byName[k] = entry
	}

	return &entry.Value.val
}

// Release implements Cache.
func (c *lruCache[K, V]) Release(k K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.byName[k]
	if !ok || entry.Value.refs <= 0 {
		panic(fmt.Errorf("caching.lruCache.Release called on key that is not held: %v", k))
	}
	entry.Value.refs--
	if entry.Value.refs == 0 {
		del := entry.Value.del != nil
		if del {
			close(entry.Value.del)
			entry.Value.del = nil
		}
		if c.waiters.IsEmpty() {
			// Add it to the free-list.
			if del {
				delete(c.byName, k)
				c.unused.Store(entry)
			} else {
				c.evictable.Store(entry)
			}
		} else {
			// Someone's waiting to pop something off of the
			// free-list; bypass the free-list and hand it directly
			// to them.

			// Make sure that no one aquires this entry between us
			// writing it to the channel and the waiter calling
			// c.mu.Lock().
			delete(c.byName, k)

			// Pass it to the waiter.
			waiter := c.waiters.Oldest()
			c.waiters.Delete(waiter)
			waiter.Value <- entry
		}
	}
}

// Delete implements Cache.
func (c *lruCache[K, V]) Delete(k K) {
	c.mu.Lock()

	entry, ok := c.byName[k]
	if !ok {
		return
	}
	if entry.Value.refs == 0 {
		delete(c.byName, k)
		if c.waiters.IsEmpty() {
			c.unused.Store(entry)
		} else {
			waiter := c.waiters.Oldest()
			c.waiters.Delete(waiter)
			waiter.Value <- entry
		}
		c.mu.Unlock()
	} else {
		if entry.Value.del == nil {
			entry.Value.del = make(chan struct{})
		}
		ch := entry.Value.del
		c.mu.Unlock()
		<-ch
	}
}

// Flush implements Cache.
func (c *lruCache[K, V]) Flush(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, entry := range c.byName {
		c.src.Flush(ctx, &entry.Value.val)
	}
	for entry := c.unused.Oldest(); entry != nil; entry = entry.Newer() {
		c.src.Flush(ctx, &entry.Value.val)
	}
}
