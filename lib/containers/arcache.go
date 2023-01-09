// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"sync"
)

// ARCache is a thread-safe Adaptive Replacement Cache.
//
// The Adaptive Replacement Cache is patented by IBM (patent
// US-6,996,676-B2 is set to expire 2024-02-22).
//
// This implementation does NOT make use of the enhancements in ZFS'
// enhanced ARC, which are patented by Sun (now Oracle) (patent
// US-7,469,320-B2 is set to expire 2027-02-13).
//
// It is invalid to adjust any public members after first use.
type ARCache[K comparable, V any] struct {
	MaxLen   int        // must be >= 2
	New      func(K) V  // may be nil (.Load)
	OnHit    func(K, V) // may be nil (.Load)
	OnMiss   func(K)    // may be nil (.Load)
	OnEvict  func(K, V) // may be nil (.Load, .Store)
	OnRemove func(K, V) // may be nil (.Load, .Store, .Delete)

	mu sync.RWMutex
	// Some of the ARC literature calls these "MRU" and "MFU" for
	// "most {recently,frequently} used", but that's wrong.  The
	// `frequent` list is still ordered by most-recent-use.  The
	// distinction is that the `recent` list is entries that have
	// been used "only once recently", while the `frequent` list
	// is entries that have been used "at least twice recently"
	// (to quote the definitions from the original ARC paper); the
	// affect being that the `recent` list is for
	// recently-but-not-frequently used entries, while the
	// `frequent` list is there to ensure that frequently-used
	// entries don't get evicted.  They're both still MRU lists.
	recentLive    lruCache[K, V]
	recentGhost   lruCache[K, struct{}]
	frequentLive  lruCache[K, V]
	frequentGhost lruCache[K, struct{}]
	// recentLiveTarget is the "target" len of recentLive.  We
	// allow the actual len to deviate from this if the ARCache as
	// a whole isn't over-len.  That is: recentLiveTarget is used
	// to decide *which* list to evict from, not *whether* we need
	// to evict.
	recentLiveTarget int

	// Add a no-op .check() method that the tests can override.
	noopChecker //nolint:unused // False positive; it is used.
}

var _ Map[int, string] = (*ARCache[int, string])(nil)

//nolint:unused // False positive; it is used.
type noopChecker struct{}

//nolint:unused // False positive; it is used.
func (noopChecker) check() {}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func bound(low, val, high int) int {
	if val < low {
		return low
	}
	if val > high {
		return high
	}
	return val
}

func (c *ARCache[K, V]) onLRUEvictRecent(k K, v V) {
	if c.recentLive.Len() < c.MaxLen {
		for c.recentLen() >= c.MaxLen {
			c.recentGhost.EvictOldest()
		}
		for c.ghostLen() >= c.MaxLen {
			if c.recentGhost.Len() > 0 {
				c.recentGhost.EvictOldest()
			} else {
				c.frequentGhost.EvictOldest()
			}
		}
		c.recentGhost.Store(k, struct{}{})
	}
	if c.OnRemove != nil {
		c.OnRemove(k, v)
	}
	if c.OnEvict != nil {
		c.OnEvict(k, v)
	}
}

func (c *ARCache[K, V]) onLRUEvictFrequent(k K, v V) {
	if c.frequentLive.Len() < c.MaxLen {
		for c.frequentLen() >= c.MaxLen {
			c.frequentGhost.EvictOldest()
		}
		for c.ghostLen() >= c.MaxLen {
			if c.frequentGhost.Len() > 0 {
				c.frequentGhost.EvictOldest()
			} else {
				c.recentGhost.EvictOldest()
			}
		}
		c.frequentGhost.Store(k, struct{}{})
	}
	if c.OnRemove != nil {
		c.OnRemove(k, v)
	}
	if c.OnEvict != nil {
		c.OnEvict(k, v)
	}
}

func (c *ARCache[K, V]) init() {
	c.check()
	if c.recentLive.OnEvict == nil {
		c.recentLive.OnEvict = c.onLRUEvictRecent
		c.frequentLive.OnEvict = c.onLRUEvictFrequent
	}
	c.check()
}

func (c *ARCache[K, V]) liveLen() int     { return c.recentLive.Len() + c.frequentLive.Len() }
func (c *ARCache[K, V]) ghostLen() int    { return c.recentGhost.Len() + c.frequentGhost.Len() }
func (c *ARCache[K, V]) recentLen() int   { return c.recentLive.Len() + c.recentGhost.Len() }
func (c *ARCache[K, V]) frequentLen() int { return c.frequentLive.Len() + c.frequentGhost.Len() }
func (c *ARCache[K, V]) fullLen() int {
	return c.recentLive.Len() + c.recentGhost.Len() + c.frequentLive.Len() + c.frequentGhost.Len()
}

// Store a key/value pair in to the cache.
func (c *ARCache[K, V]) Store(k K, v V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.init()

	c.unlockedStore(k, v)
}

func (c *ARCache[K, V]) unlockedStore(k K, v V) {
	// The "Case" comments here reflect Fig. 4. in the original paper "ARC: A
	// Self-Tuning, Low Overhead Replacement Cache" by N. Megiddo & D. Modha, FAST
	// 2003.
	switch {
	case c.recentLive.Has(k): // Case I(a): cache hit
		// Make room
		for c.frequentLen() >= c.MaxLen {
			if c.frequentGhost.Len() > 0 {
				c.frequentGhost.EvictOldest()
			} else {
				c.frequentLive.EvictOldest()
			}
		}
		c.check()
		// Move
		oldV, _ := c.recentLive.Peek(k)
		c.recentLive.Delete(k)
		c.frequentLive.Store(k, v)
		if c.OnRemove != nil {
			c.OnRemove(k, oldV)
		}
		c.check()
	case c.frequentLive.Has(k): // Case I(b): cache hit
		oldV, _ := c.frequentLive.Peek(k)
		c.frequentLive.Store(k, v)
		if c.OnRemove != nil {
			c.OnRemove(k, oldV)
		}
		c.check()
	case c.recentGhost.Has(k): // Case II: cache miss (that "should" have been a hit)
		// Adapt
		c.recentLiveTarget = bound(
			0,
			c.recentLiveTarget+max(1, c.frequentGhost.Len()/c.recentGhost.Len()),
			c.MaxLen)
		// Make room
		for c.liveLen() >= c.MaxLen {
			if c.recentLive.Len() > c.recentLiveTarget {
				c.recentLive.EvictOldest()
			} else {
				c.frequentLive.EvictOldest()
			}
		}
		for c.frequentLen() >= c.MaxLen {
			if c.frequentGhost.Len() > 0 {
				c.frequentGhost.EvictOldest()
			} else {
				c.frequentLive.EvictOldest()
			}
		}
		c.check()
		// Store
		c.recentGhost.Delete(k)
		c.frequentLive.Store(k, v)
		c.check()
	case c.frequentGhost.Has(k): // Case III: cache miss (that "should" have been a hit)
		// Adapt
		c.recentLiveTarget = bound(
			0,
			c.recentLiveTarget-max(1, c.recentGhost.Len()/c.frequentGhost.Len()),
			c.MaxLen)
		// Make room
		for c.liveLen() >= c.MaxLen {
			// TODO(lukeshu): I don't understand why this .recentLiveTarget
			// check needs to be `>=` instead of `>` like all of the others.
			if c.recentLive.Len() >= c.recentLiveTarget && c.recentLive.Len() > 0 {
				c.recentLive.EvictOldest()
			} else {
				c.frequentLive.EvictOldest()
			}
		}
		c.check()
		// Store
		c.frequentGhost.Delete(k)
		c.frequentLive.Store(k, v)
		c.check()
	default: // Case IV: cache miss
		// Make room
		if c.recentLen() < c.MaxLen {
			for c.liveLen() >= c.MaxLen {
				if c.recentLive.Len() > c.recentLiveTarget {
					c.recentLive.EvictOldest()
				} else {
					c.frequentLive.EvictOldest()
				}
			}
		} else {
			c.recentLive.EvictOldest()
			c.recentGhost.EvictOldest()
		}
		c.check()
		// Store
		c.recentLive.Store(k, v)
		c.check()
	}
}

// Load an entry from the cache, recording a "use" for the purposes of
// "least-recently-used" eviction.
//
// Calls .OnHit or .OnMiss depending on whether it's a cache-hit or
// cache-miss.
//
// If .New is non-nil, then .Load will never return (zero, false).
func (c *ARCache[K, V]) Load(k K) (v V, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.init()
	defer c.check()

	if v, ok := c.recentLive.Peek(k); ok {
		// Make room
		for c.frequentLen() >= c.MaxLen {
			if c.frequentGhost.Len() > 0 {
				c.frequentGhost.EvictOldest()
			} else {
				c.frequentLive.EvictOldest()
			}
		}
		// Store
		c.recentLive.Delete(k)
		c.frequentLive.Store(k, v)
		if c.OnHit != nil {
			c.OnHit(k, v)
		}
		return v, true
	}
	if v, ok := c.frequentLive.Load(k); ok {
		if c.OnHit != nil {
			c.OnHit(k, v)
		}
		return v, true
	}

	if c.OnMiss != nil {
		c.OnMiss(k)
	}
	if c.New != nil {
		v := c.New(k)
		c.unlockedStore(k, v)
		return v, true
	}
	var zero V
	return zero, false
}

// Delete an entry from the cache.
func (c *ARCache[K, V]) Delete(k K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	v, ok := c.unlockedPeek(k)

	c.recentLive.Delete(k)
	c.recentGhost.Delete(k)
	c.frequentLive.Delete(k)
	c.frequentGhost.Delete(k)

	if ok && c.OnRemove != nil {
		c.OnRemove(k, v)
	}
}

// Peek is like Load, but doesn't count as a "use" for
// "least-recently-used".
func (c *ARCache[K, V]) Peek(k K) (v V, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.unlockedPeek(k)
}

func (c *ARCache[K, V]) unlockedPeek(k K) (v V, ok bool) {
	if v, ok := c.recentLive.Peek(k); ok {
		return v, true
	}

	return c.frequentLive.Peek(k)
}

// Has returns whether an entry is present in the cache.  Calling Has
// does not count as a "use" for "least-recently-used".
func (c *ARCache[K, V]) Has(k K) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.recentLive.Has(k) || c.frequentLive.Has(k)
}

// Len returns the number of entries in the cache.
func (c *ARCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.liveLen()
}
