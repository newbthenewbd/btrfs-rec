// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// This file should be reasonably readable from top-to-bottom; I've
// tried to write it in a sort-of "literate programming" style.  That
// makes the file comparatively huge--but don't let that intimidate
// you, it's only huge because of the detailed comments; it's less
// than 300 lines without the comments.

package containers

import (
	"context"
	"fmt"
	"sync"
)

// NewARCache returns a new thread-safe Adaptive Replacement Cache
// (ARC).
//
// Fundamentally, the point of ARC is to combine both recency
// information and frequency information together to form a cache
// policy that is better than both least-recently-used eviction (LRU)
// and least-frequently-used eviction (LFU); and the balance between
// how much weight is given to recency vs frequency is "adaptive"
// based on the characteristics of the current workload.
//
// The Adaptive Replacement Cache is patented by IBM (patent
// US-6,996,676-B2 is set to expire 2024-02-22).
//
// This implementation does NOT make use of the enhancements in ZFS'
// enhanced ARC, which are patented by Sun (now Oracle) (patent
// US-7,469,320-B2 is set to expire 2027-02-13).
//
// This implementation has a few enhancements compared to standard
// ARC:
//
//   - This implementation supports explicitly deleting/invalidating
//     cache entries; the standard ARC algorithm assumes that the only
//     reason an entry is ever removed from the cache is because the
//     cache is full and the entry had to be evicted to make room for
//     a new entry.
//
//   - This implementation supports pinning entries such that they
//     cannot be evicted.  This is one of the enhancement from the
//     enhanced version of ARC used by ZFS, but the version here is
//     not based on the ZFS version.
//
// It is invalid (runtime-panic) to call NewARCache with a
// non-positive capacity or a nil source.
//
//nolint:predeclared // 'cap' is the best name for it.
func NewARCache[K comparable, V any](cap int, src Source[K, V]) Cache[K, V] {
	// Pass the parameters in.
	if cap <= 0 {
		panic(fmt.Errorf("containers.NewARCache: invalid capacity: %v", cap))
	}
	if src == nil {
		panic(fmt.Errorf("containers.NewARCache: nil source"))
	}
	ret := &arCache[K, V]{
		cap: cap,
		src: src,
		// Do allocations up-front.  Don't yet worry about
		// what these members are; we'll get to that in the
		// below description of the datatypes.
		liveByName:  make(map[K]*LinkedListEntry[arcLiveEntry[K, V]], cap),
		ghostByName: make(map[K]*LinkedListEntry[arcGhostEntry[K]], cap),
	}
	for i := 0; i < cap; i++ {
		ret.unusedLive.Store(new(LinkedListEntry[arcLiveEntry[K, V]]))
		ret.unusedGhost.Store(new(LinkedListEntry[arcGhostEntry[K]]))
	}
	// Return.
	return ret
}

// Related literature:
//
//   The comments in this file use terminology from the original ARC
//   paper: "ARC: A Self-Tuning, Low Overhead Replacement Cache" by
//   N. Megiddo & D. Modha, FAST 2003.
//   https://www.usenix.org/legacy/events/fast03/tech/full_papers/megiddo/megiddo.pdf
//
//   But, this file tries to be clear enough that it makes sense
//   without reading the paper.
//
//   If you do read the paper, a difference to keep an eye out for is
//   that in order to support "delete", several of the assumptions
//   related to DBL(2c) are no longer true.  Specifically, we must
//   handle the cache not being full in cases other than a DBL(2c) miss;
//   and two of the invariants from Π(c) are no longer true (see the bit
//   about invariants below).  Besides the obvious (calls to
//   synchronization primitives, things with "del" or "pin" in the
//   name), places where the standard ARC algorithm is modified to
//   support deletion or pinning should be clearly commented.
//
// Background / data structures:
//
//   `ARC(c)` -- that is, an adaptive replacement cache with capacity
//   `c` -- is most reasonably understood in terms of an "imaginary"
//   simpler `DBL(2c)` algorithm.
//
//   DBL(2c) is a cache that maintains 2c entries in a set of lists
//   ordered by LRU/MRU.  These lists are called L₁ or "recent" and L₂
//   or "frequent"; |L₁| + |L₂| ≤ 2c.  L₁/recent is for entries that
//   have only been used only once "recently", and L₂/frequent is for
//   entries that have been used twice or more "recently" (for a
//   particular definition of "recently" that we don't need to get
//   into yet).
//
//     Aside: Some of the ARC literature calls these lists "MRU" and
//     "MFU" for "most {recently,frequently} used", but that's wrong.
//     The L₂/frequent list is still ordered by recency of use.
//
//   Put another way, L₁/recent is an recency-ordered list of
//   recently-but-not-frequently-used entries, while L₂/frequent is an
//   recency-ordered list of frequently-used entries.
//
//   We'll get to DBL(2c)'s replacement algorithm later; the above
//   "shape" is enough of an introduction for now.
//
//   Now, the difference of ARC(c) over DBL(2c) is that ARC(c) splits
//   those lists into segments; L₁ gets split into a "top" part T₁ and
//   a "bottom" part B₁, and similarly L₂ gets split into a "top" part
//   T₂ and a "bottom" part B₂.  The "cache" is only made of T₁ and
//   T₂; entries in B₁ and B₂ are evicted; the 4 lists together make
//   up a "directory" of what would be in DBL(2c).  That is:
//
//      cache     = T₁ ∪ T₂
//      directory = T₁ ∪ T₂ ∪ B₁ ∪ B₂
//      L₁        = T₁ ∪ B₁
//      L₂        = T₂ ∪ B₂
//
//   Let us say that entries in the T₁ or T₂ are "live entries", and
//   entries in B₁ or B₂ are "ghost entries".  The ghost entries make
//   up a record of recent evictions.  We could use the same struct
//   for live entries and ghost entries, and just have everything but
//   the key zeroed-out for ghost entries; but to simplify things
//   let's just have different structures:

type arcLiveEntry[K comparable, V any] struct {
	key K
	val V

	refs int           // for pinning
	del  chan struct{} // non-nil if a delete is waiting on .refs to drop to zero
}

type arcGhostEntry[K comparable] struct {
	key K
}

type arCache[K comparable, V any] struct {
	cap int // "c"
	src Source[K, V]

	mu sync.RWMutex

	// Now, the above was a sort of lie for this implementation;
	// for our pinning implementation, we actually segment L₁ and
	// L₂ into *three* segments rather than two segments: the top
	// part is pinned (and thus live) entries, the middle part is
	// live-but-not-pinned entries, and the bottom part is ghost
	// entries.
	//
	// We don't actually care about the order of the pinned
	// entries (the lists are ordered by recency-of-use, and
	// pinned entries are all "in-use", so they're all tied), but
	// it's convenient to maintain the set of them as sorted lists
	// the same as everything else.

	// L₁ / recently-but-not-frequently used entries
	recentPinned LinkedList[arcLiveEntry[K, V]] // top of L₁
	recentLive   LinkedList[arcLiveEntry[K, V]] // "T₁" for "top of L₁" (but really the middle)
	recentGhost  LinkedList[arcGhostEntry[K]]   // "B₁" for "bottom of L₁"

	// L₂ / frequently used entries
	frequentPinned LinkedList[arcLiveEntry[K, V]] // top of L₂
	frequentLive   LinkedList[arcLiveEntry[K, V]] // "T₂" for "top of L₂" (but really the middle)
	frequentGhost  LinkedList[arcGhostEntry[K]]   // "B₂" for "bottom of L₂"

	// Now, where to draw the split between the "live" part and
	// "ghost" parts of each list?  We'll use a parameter
	// "p"/recentLiveTarget to decide which list to evict
	// (transition live→ghost) from whenever we need to do an
	// eviction.
	//
	// recentLiveTarget is the "target" len of
	// recentPinned+recentLive.  We allow the actual len to
	// deviate from this if the arCache as a whole isn't
	// over-capacity.  To say it again: recentLiveTarget is used
	// to decide *which* list to evict from, not *whether* we need
	// to evict.
	//
	// recentLiveTarget is always in the range [0, cap]; it never
	// goes negative, and never goes beyond cap.  Adjusting this
	// target is the main way that ARC is "adaptive", we could
	// instead define a "fixed replacement cache" policy FRC(p, c)
	// that has a static target.  But we'll get into that later.
	recentLiveTarget int // "p"

	// Other book-keeping.

	// For lookups.  The above ordered lists are about
	// eviction/replacement policies, but they don't help us when
	// we want to read something from the cache; we'd have to do
	// an O(n) scan through each list to find the item we're
	// looking for.  So maintain this separate index in order to
	// do O(1) lookups when we want to read from the cache.
	liveByName  map[K]*LinkedListEntry[arcLiveEntry[K, V]]
	ghostByName map[K]*LinkedListEntry[arcGhostEntry[K]]

	// Free lists.  Like the pinned lists, we don't actually care
	// about the order here, it's just convenient to use the same
	// ordered lists.
	unusedLive  LinkedList[arcLiveEntry[K, V]]
	unusedGhost LinkedList[arcGhostEntry[K]]

	// For blocking related to pinning.
	waiters LinkedList[chan struct{}]
}

// Algorithms:
//
//   Now that all of our data structures are defined, let's get into
//   the algorithms of updating them.
//
//   Before getting to the meaty ARC stuff, let's get some
//   boring/simple synchronization/blocking primitives out of the way:

// waitForAvail is called before storing something into the cache.
// This is nescessary because if the cache is full and all entries are
// pinned, then we won't be able to store the entry until something
// gets unpinned ("Release()d").
func (c *arCache[K, V]) waitForAvail() {
	if !(c.recentLive.IsEmpty() && c.frequentLive.IsEmpty() && c.unusedLive.IsEmpty()) {
		// There is already an available `arcLiveEntry` that
		// we can either use or evict.
		return
	}
	ch := make(chan struct{})
	c.waiters.Store(&LinkedListEntry[chan struct{}]{Value: ch})
	c.mu.Unlock()
	<-ch // receive the lock from .Release()
	if c.recentLive.IsEmpty() && c.frequentLive.IsEmpty() && c.unusedLive.IsEmpty() {
		panic(fmt.Errorf("should not happen: waitForAvail is returning, but nothing is available"))
	}
}

// unlockAndNotifyAvail is called when an entry gets unpinned
// ("Release()d"), and wakes up the highest-priority .waitForAvail()
// waiter (if there is one).
func (c *arCache[K, V]) unlockAndNotifyAvail() {
	waiter := c.waiters.Oldest
	if waiter == nil {
		c.mu.Unlock()
		return
	}
	c.waiters.Delete(waiter)
	// We don't actually unlock, we're "transferring" the lock to
	// the waiter.
	close(waiter.Value)
}

// Calling .Delete(k) on an entry that is pinned needs to block until
// the entry is no longer pinned.
func (c *arCache[K, V]) unlockAndWaitForDel(entry *LinkedListEntry[arcLiveEntry[K, V]]) {
	if entry.Value.del == nil {
		entry.Value.del = make(chan struct{})
	}
	ch := entry.Value.del
	c.mu.Unlock()
	<-ch
}

// notifyOfDel unblocks any calls to .Delete(k), notifying them that
// the entry has been deleted and they can now return.
func (*arCache[K, V]) notifyOfDel(entry *LinkedListEntry[arcLiveEntry[K, V]]) {
	if entry.Value.del != nil {
		close(entry.Value.del)
		entry.Value.del = nil
	}
}

//   OK, now to the main algorithm(s)!
//
//   To get this out of the way: Here are the invariants that the
//   algorithm(s) must enforce (see the paper for justification):
//
//     from DBL(2c):
//
//        • 0 ≤ |L₁|+|L₂| ≤ 2c
//        • 0 ≤ |L₁| ≤ c
//        • 0 ≤ |L₂| ≤ 2c
//
//     from Π(c):
//
//       Π(c) is the class of policies that ARC(c) belongs to... I
//       suppose that because of the changes we make to support
//       deletion, this implementation doesn't actually belong to
//       Π(c).
//
//        • A.1: The lists T₁, B₁, T₂, and B₂ are all mutually
//          disjoint.
//        • (not true) A.2: If |L₁|+|L₂| < c, then both B₁ and B₂ are
//          empty.  But supporting "delete" invalidates this!
//        • (not true) A.3: If |L₁|+|L₂| ≥ c, then |T₁|+|T₂| = c.  But
//          supporting "delete" invalidates this!
//        • A.4(a): Either (T₁ or B₁ is empty), or (the LRU page in T₁
//          is more recent than the MRU page in B₁).
//        • A.4(b): Either (T₂ or B₂ is empty), or (the LRU page in T₂
//          is more recent than the MRU page in B₂).
//        • A.5: |T₁∪T₂| is the set of pages that would be maintained
//          by the cache policy π(c).
//
//       The algorithm presented in the paper relies on A.2 and A.3 in
//       order to be correct; the algorithm had to be adjusted in
//       order to behave properly without relying on those two
//       invariants.
//
//     from FRC(p, c):
//
//        • 0 ≤ p ≤ c
//
//   OK, at the beginning I said that ARC(c) is most reasonably
//   understood in terms of DBL(2c); to that end, we'll have two
//   replacement policies that are layered; the "dblReplace" policy
//   that is used in the cases when DBL(2c) would have a cache-miss,
//   and the "arcReplace" policy that is used when ARC(c) has a
//   cache-miss but DBL(2c) wouldn't have (and within dblReplace).

// dblReplace is the DBL(2c) replacement policy.
//
// It returns an entry that is not in any list (c.recentPinned,
// c.recentLive, c.frequentPinned, c.frequentLive, or c.unusedLive),
// and is ready to be stored into a list by the caller.
func (c *arCache[K, V]) dblReplace() *LinkedListEntry[arcLiveEntry[K, V]] {
	c.waitForAvail()

	// The DBL(2c) replacement policy is quite simple: "Replace
	// the LRU page in L₁, if L₁ contains exactly c pages;
	// otherwise, replace the LRU page in L₂"
	//
	// This looks a touch more complicated than a simple DBL(2c)
	// implementation would look, but that's just because L₁ and
	// L₂ are not implemented as uniform lists; instead of "the
	// LRU entry of L₁" being a simple `c.recent.Oldest`, we have
	// to check the 3 different segments of L₁.

	recentLen := c.recentPinned.Len + c.recentLive.Len + c.recentGhost.Len // |L₁|
	switch {
	case recentLen == c.cap:
		// Use the LRU entry from L₁.
		//
		// Note that *even when* there are available entries
		// from c.unusedLive, we still do this and evict the
		// LRU entry from L₁ in order to avoid violating the
		// `0 ≤ |L₁| ≤ c` invariant.
		switch {
		case !c.recentGhost.IsEmpty(): // bottom
			return c.arcReplace(c.recentGhost.Oldest, true, false)
		case !c.recentLive.IsEmpty(): // middle
			entry := c.recentLive.Oldest
			c.recentLive.Delete(entry)
			delete(c.liveByName, entry.Value.key)
			return entry
		default: // case !c.recentPinned.IsEmpty(): // top

			// This can't happen because `c.recentLen == c.cap &&
			// c.recentGhost.IsEmpty() && c.recentLive.IsEmpty()`
			// implies that `c.recentPinned.Len == c.cap`, which
			// can't be true if c.waitForAvail() returned.
			panic(fmt.Errorf("should not happen: lengths don't match up"))
		}
	case recentLen < c.cap:
		// If the directory is full, use the LRU entry from
		// L₂; otherwise use a free (unused) entry.
		switch {
		// Cache is not full: use a free entry.
		case !c.unusedLive.IsEmpty():
			entry := c.unusedLive.Oldest
			c.unusedLive.Delete(entry)
			return entry
		case !c.unusedGhost.IsEmpty():
			return c.arcReplace(c.unusedGhost.Oldest, false, false)
		// Cache is full: use the LRU entry from L₂
		case !c.frequentGhost.IsEmpty():
			return c.arcReplace(c.frequentGhost.Oldest, false, false)
		default:
			// This can't happen because `recentLen < c.cap` implies
			// that `c.recentGhost.Len < c.cap`, which means that
			// there is at least one ghost entry that is available
			// in c.unusedGhost or c.frequentGhost.
			panic(fmt.Errorf("should not happen: lengths don't match up"))
		}
	default: // case recentLen > c.cap:
		// Can't happen because `0 ≤ |L₁| ≤ c` is one of the
		// invariants from DBL(2c); the above policy will
		// never let it happen.
		panic(fmt.Errorf("should not happen: recentLen:%v > cap:%v", recentLen, c.cap))
	}
}

// arcReplace is the ARC(c) replacement policy.
//
// It returns an entry that is not in any list (c.recentPinned,
// c.recentLive, c.frequentPinned, c.frequentLive, or c.unusedLive),
// and is ready to be stored into a list by the caller.
//
// If an eviction is performed, `ghostEntry` is a pointer to the entry
// object that is used as a record of the eviction.  `ghostEntry`
// should still be present in its old list, in case an eviction is not
// performed and the `ghostEntry` object is not modified.
//
// The `arbitrary` argument is arbitrary, see the quote about it
// below.
func (c *arCache[K, V]) arcReplace(ghostEntry *LinkedListEntry[arcGhostEntry[K]], forceEviction, arbitrary bool) *LinkedListEntry[arcLiveEntry[K, V]] {
	c.waitForAvail()

	// If the cache isn't full, no need to do an eviction.  (This
	// check is a nescessary enhancement over standard ARC in
	// order to support "delete"; because without "delete",
	// arcReplace wouldn't ever be called when the cache isn't
	// full.)
	if !c.unusedLive.IsEmpty() && !forceEviction {
		entry := c.unusedLive.Oldest
		c.unusedLive.Delete(entry)
		return entry
	}

	// We'll be evicting.  Prep ghostEntry to record that fact.
	if ghostEntry.List != &c.unusedGhost {
		delete(c.ghostByName, ghostEntry.Value.key)
	}
	ghostEntry.List.Delete(ghostEntry)

	// Note that from here on out, this policy changes *neither*
	// |L₁| nor |L₂|; shortenings were already done by the above
	// `ghostEntry.List.Delete(ghostEntry)` call, and lengthenings
	// will be done by the caller with the returned `entry`.  All
	// this policy is doing from here on out is changing the split
	// between T/B.

	// We have to make a binary choice about whether to evict
	// c.recentLive→c.recentGhost or
	// c.frequentLive→c.frequentGhost.
	var evictFrom *LinkedList[arcLiveEntry[K, V]]
	var evictTo *LinkedList[arcGhostEntry[K]]

	// Make the decision.
	recentLive := c.recentPinned.Len + c.recentLive.Len
	switch { // NB: Also check .IsEmpty() in order to support pinning.
	case recentLive > c.recentLiveTarget && !c.recentLive.IsEmpty():
		evictFrom, evictTo = &c.recentLive, &c.recentGhost
	case recentLive < c.recentLiveTarget && !c.frequentLive.IsEmpty():
		evictFrom, evictTo = &c.frequentLive, &c.frequentGhost
	default: // case recentLive == c.recentLiveTarget || the_normal_list_was_empty:

		// The original paper says "The last replacement
		// decision is somewhat arbitrary, and can be made
		// differently if desired."
		if arbitrary && !c.recentLive.IsEmpty() {
			evictFrom, evictTo = &c.recentLive, &c.recentGhost
		} else {
			evictFrom, evictTo = &c.frequentLive, &c.frequentGhost
		}
	}

	// Act on the decision.
	entry := evictFrom.Oldest
	// Evict.
	delete(c.liveByName, entry.Value.key)
	evictFrom.Delete(entry)
	// Record the eviction.
	ghostEntry.Value.key = entry.Value.key
	evictTo.Store(ghostEntry)
	c.ghostByName[ghostEntry.Value.key] = ghostEntry

	return entry
}

//   OK, now that we have our replacement policies defined, it's
//   pretty obvious how to wire them into the "acquire an entry"
//   algorithm.  The only parts here that aren't obvious are:
//
//     - the "adapt" step that adjusts c.recentLiveTarget.  Read the
//       paper for an explanation of why the formulas used do a good
//       job of quickly adapting to various workloads.
//
//     - the `ghostEntry.List == &c.frequentGhost` argument to
//       arcReplace in the "cache-miss, but would have been a
//       cache-hit in DBL(2c)" case.  The short answer is that it's
//       arbitrary (as discussed in comments in arcReplace), but
//       matches what's in the original paper.

// Acquire implements the 'Cache' interface.
func (c *arCache[K, V]) Acquire(ctx context.Context, k K) *V {
	c.mu.Lock()
	defer c.mu.Unlock()

	var entry *LinkedListEntry[arcLiveEntry[K, V]]
	switch {
	case c.liveByName[k] != nil: // cache-hit
		entry = c.liveByName[k]
		// Move to frequentPinned, unless:
		//
		//  - it's already there; in which case, don't bother
		//  - it's in recentPinned; don't count "nested" uses
		//    as "frequent" uses.
		if entry.List != &c.frequentPinned && entry.List != &c.recentPinned {
			entry.List.Delete(entry)
			c.frequentPinned.Store(entry)
		}
		entry.Value.refs++
	case c.ghostByName[k] != nil: // cache-miss, but would have been a cache-hit in DBL(2c)
		ghostEntry := c.ghostByName[k]
		// Adapt.
		switch ghostEntry.List {
		case &c.recentGhost:
			// Recency is doing well right now; invest toward recency.
			c.recentLiveTarget = min(c.recentLiveTarget+max(1, c.frequentGhost.Len/c.recentGhost.Len), c.cap)
		case &c.frequentGhost:
			// Frequency is doing well right now; invest toward frequency.
			c.recentLiveTarget = max(c.recentLiveTarget-max(1, c.recentGhost.Len/c.frequentGhost.Len), 0)
		}
		// Whether or not we do an eviction, this ghost entry
		// needs to go away.
		ghostEntry.List.Delete(ghostEntry)
		delete(c.ghostByName, k)
		c.unusedGhost.Store(ghostEntry)
		// Replace.
		entry = c.arcReplace(ghostEntry, false, ghostEntry.List == &c.frequentGhost)
		entry.Value.key = k
		c.src.Load(ctx, k, &entry.Value.val)
		entry.Value.refs = 1
		c.frequentPinned.Store(entry)
		c.liveByName[k] = entry
	default: // cache-miss, and would have even been a cache-miss in DBL(2c)
		// Replace.
		entry = c.dblReplace()
		entry.Value.key = k
		c.src.Load(ctx, k, &entry.Value.val)
		entry.Value.refs = 1
		c.recentPinned.Store(entry)
		c.liveByName[k] = entry
	}
	return &entry.Value.val
}

//   Given everything that we've already explained, I think it's fair to call
//   the remaining code "boilerplate".

// Delete implements the 'Cache' interface.
func (c *arCache[K, V]) Delete(k K) {
	c.mu.Lock()

	if entry := c.liveByName[k]; entry != nil {
		if entry.Value.refs > 0 {
			// Let .Release(k) do the deletion when the
			// refcount drops to 0.
			c.unlockAndWaitForDel(entry)
			return
		}
		delete(c.liveByName, entry.Value.key)
		entry.List.Delete(entry)
		c.unusedLive.Store(entry)
	} else if entry := c.ghostByName[k]; entry != nil {
		delete(c.ghostByName, k)
		entry.List.Delete(entry)
		c.unusedGhost.Store(entry)
	}

	// No need to call c.unlockAndNotifyAvail(); if we were able
	// to delete it, it was already available.

	c.mu.Unlock()
}

// Release implements the 'Cache' interface.
func (c *arCache[K, V]) Release(k K) {
	c.mu.Lock()

	entry := c.liveByName[k]
	if entry == nil || entry.Value.refs <= 0 {
		panic(fmt.Errorf("containers.arCache.Release called on key that is not held: %v", k))
	}

	entry.Value.refs--
	if entry.Value.refs == 0 {
		switch {
		case entry.Value.del != nil:
			delete(c.liveByName, entry.Value.key)
			entry.List.Delete(entry)
			c.unusedLive.Store(entry)
			c.notifyOfDel(entry)
		case entry.List == &c.recentPinned:
			c.recentPinned.Delete(entry)
			c.recentLive.Store(entry)
		case entry.List == &c.frequentPinned:
			c.frequentPinned.Delete(entry)
			c.frequentLive.Store(entry)
		default:
			panic(fmt.Errorf("should not happen: entry is not pending deletion, and is not in a pinned list"))
		}
		c.unlockAndNotifyAvail()
	} else {
		c.mu.Unlock()
	}
}

// Flush implements the 'Cache' interface.
func (c *arCache[K, V]) Flush(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, list := range []*LinkedList[arcLiveEntry[K, V]]{
		&c.recentPinned,
		&c.recentLive,
		&c.frequentPinned,
		&c.frequentLive,
		&c.unusedLive,
	} {
		for entry := list.Oldest; entry != nil; entry = entry.Newer {
			c.src.Flush(ctx, &entry.Value.val)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
