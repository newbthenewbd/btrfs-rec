// Copyright (C) 2015, 2022  HashiCorp, Inc.
// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: MPL-2.0
//
// Based on https://github.com/hashicorp/golang-lru/blob/efb1d5b30f66db326f4d8e27b3a5ad04f5e02ca3/arc_test.go

package containers

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"sort"
	"testing"

	"github.com/datawire/dlib/derror"
	"github.com/datawire/dlib/dlog"
	"github.com/stretchr/testify/require"
)

// Add runtime validity checks /////////////////////////////////////////////////

func (c *arc[K, V]) logf(format string, a ...any) {
	c.t.Helper()
	c.t.Logf("%[1]T(%[1]p): %s (b1:%v t1:%v p1:%v / p1:%v, t2:%v b2:%v)",
		c,
		fmt.Sprintf(format, a...),
		c.recentGhost.Len,
		c.recentLive.Len,
		c.recentPinned.Len,
		c.frequentPinned.Len,
		c.frequentLive.Len,
		c.frequentGhost.Len)
}

func (c *arc[K, V]) fatalf(format string, a ...any) {
	c.logf(format, a...)
	c.t.FailNow()
}

func (c *arc[K, V]) check() {
	if c.noCheck {
		return
	}
	c.t.Helper()

	// Do the slow parts for 1/32 of all calls.
	fullCheck := getRand(c.t, 32) == 0

	// Check that the lists are in-sync with the maps.
	if fullCheck {
		liveEntries := make(map[*LinkedListEntry[arcLiveEntry[K, V]]]int, len(c.liveByName))
		for _, list := range c.liveLists {
			for entry := list.Oldest; entry != nil; entry = entry.Newer {
				liveEntries[entry]++
			}
		}
		for _, entry := range c.liveByName {
			liveEntries[entry]--
			if liveEntries[entry] == 0 {
				delete(liveEntries, entry)
			}
		}
		require.Len(c.t, liveEntries, 0)

		ghostEntries := make(map[*LinkedListEntry[arcGhostEntry[K]]]int, len(c.ghostByName))
		for _, list := range c.ghostLists {
			for entry := list.Oldest; entry != nil; entry = entry.Newer {
				ghostEntries[entry]++
			}
		}
		for _, entry := range c.ghostByName {
			ghostEntries[entry]--
			if ghostEntries[entry] == 0 {
				delete(ghostEntries, entry)
			}
		}
		require.Len(c.t, ghostEntries, 0)
	}

	// Check the invariants.

	// from DBL(2c):
	//
	//    • 0 ≤ |L₁|+|L₂| ≤ 2c
	if fullLen := c.recentPinned.Len + c.recentLive.Len + c.recentGhost.Len + c.frequentPinned.Len + c.frequentLive.Len + c.frequentGhost.Len; fullLen < 0 || fullLen > 2*c.cap {
		c.fatalf("! ( 0 <= fullLen:%v <= 2*cap:%v )", fullLen, c.cap)
	}
	//    • 0 ≤ |L₁| ≤ c
	if recentLen := c.recentPinned.Len + c.recentLive.Len + c.recentGhost.Len; recentLen < 0 || recentLen > c.cap {
		c.fatalf("! ( 0 <= recentLen:%v <= cap:%v )", recentLen, c.cap)
	}
	//    • 0 ≤ |L₂| ≤ 2c
	if frequentLen := c.frequentPinned.Len + c.frequentLive.Len + c.frequentGhost.Len; frequentLen < 0 || frequentLen > 2*c.cap {
		c.fatalf("! ( 0 <= frequentLen:%v <= 2*cap:%v )", frequentLen, c.cap)
	}
	//
	// from Π(c):
	//
	//    • A.1: The lists T₁, B₁, T₂, and B₂ are all mutually
	//      disjoint.
	if fullCheck {
		keys := make(map[K]int, len(c.liveByName)+len(c.ghostByName))
		for _, list := range c.liveLists {
			for entry := list.Oldest; entry != nil; entry = entry.Newer {
				keys[entry.Value.key]++
			}
		}
		for _, list := range c.ghostLists {
			for entry := list.Oldest; entry != nil; entry = entry.Newer {
				keys[entry.Value.key]++
			}
		}
		for key, cnt := range keys {
			if cnt > 1 {
				listNames := make([]string, 0, cnt)
				for listName, list := range c.liveLists {
					for entry := list.Oldest; entry != nil; entry = entry.Newer {
						if entry.Value.key == key {
							listNames = append(listNames, listName)
						}
					}
				}
				for listName, list := range c.ghostLists {
					for entry := list.Oldest; entry != nil; entry = entry.Newer {
						if entry.Value.key == key {
							listNames = append(listNames, listName)
						}
					}
				}
				sort.Strings(listNames)
				c.fatalf("dup key: %v is in %v", key, listNames)
			}
		}
	}
	//    • (not true) A.2: If |L₁|+|L₂| < c, then both B₁ and B₂ are
	//      empty.  But supporting "delete" invalidates this!
	//    • (not true) A.3: If |L₁|+|L₂| ≥ c, then |T₁|+|T₂| = c.  But
	//      supporting "delete" invalidates this!
	//    • A.4(a): Either (T₁ or B₁ is empty), or (the LRU page in T₁
	//      is more recent than the MRU page in B₁).
	//    • A.4(b): Either (T₂ or B₂ is empty), or (the LRU page in T₂
	//      is more recent than the MRU page in B₂).
	//    • A.5: |T₁∪T₂| is the set of pages that would be maintained
	//      by the cache policy π(c).
	//
	// from FRC(p, c):
	//
	//    • 0 ≤ p ≤ c
	if c.recentLiveTarget < 0 || c.recentLiveTarget > c.cap {
		c.fatalf("! ( 0 <= p:%v <= cap:%v )", c.recentLiveTarget, c.cap)
	}
}

// Compatibility layer for hashicorp/golang-lru ////////////////////////////////

type lenFunc func() int

func (fn lenFunc) Len() int { return fn() }

type arc[K comparable, V any] struct {
	*arCache[K, V]
	ctx context.Context //nolint:containedctx // have no choice to keep the hashicorp-compatible API
	t   testing.TB

	t1, t2, b1, b2 lenFunc

	// For speeding up .check()
	noCheck    bool
	liveLists  map[string]*LinkedList[arcLiveEntry[K, V]]
	ghostLists map[string]*LinkedList[arcGhostEntry[K]]
}

func NewARC[K comparable, V any](t testing.TB, size int) (*arc[K, V], error) {
	src := SourceFunc[K, V](func(context.Context, K, *V) {})
	_, isBench := t.(*testing.B)
	ret := &arc[K, V]{
		ctx:     dlog.NewTestContext(t, true),
		t:       t,
		noCheck: isBench,
	}
	ret.init(size, src)
	return ret, nil
}

func (c *arc[K, V]) init(size int, src Source[K, V]) {
	c.arCache = NewARCache[K, V](size, src).(*arCache[K, V])
	c.t1 = lenFunc(func() int { return c.arCache.recentLive.Len })
	c.t2 = lenFunc(func() int { return c.arCache.frequentLive.Len })
	c.b1 = lenFunc(func() int { return c.arCache.recentGhost.Len })
	c.b2 = lenFunc(func() int { return c.arCache.frequentGhost.Len })

	c.liveLists = map[string]*LinkedList[arcLiveEntry[K, V]]{
		"p1": &c.recentPinned,
		"t1": &c.recentLive,
		"p2": &c.frequentPinned,
		"t2": &c.frequentLive,
	}
	c.ghostLists = map[string]*LinkedList[arcGhostEntry[K]]{
		"b1": &c.recentGhost,
		"b2": &c.frequentGhost,
	}
}

// non-mutators

func (c *arc[K, V]) p() int            { return c.recentLiveTarget }
func (c *arc[K, V]) Len() int          { return len(c.liveByName) }
func (c *arc[K, V]) Contains(k K) bool { return c.liveByName[k] != nil }

func (c *arc[K, V]) Peek(k K) (V, bool) {
	entry := c.liveByName[k]
	if entry == nil {
		var zero V
		return zero, false
	}
	return entry.Value.val, true
}

func (c *arc[K, V]) Keys() []K {
	ret := make([]K, 0, len(c.liveByName))
	for entry := c.recentLive.Oldest; entry != nil; entry = entry.Newer {
		ret = append(ret, entry.Value.key)
	}
	for entry := c.frequentLive.Oldest; entry != nil; entry = entry.Newer {
		ret = append(ret, entry.Value.key)
	}
	return ret
}

// mutators

func (c *arc[K, V]) Remove(k K) {
	defer c.check()
	c.Delete(k)
}

func (c *arc[K, V]) Purge() {
	defer c.check()
	c.init(c.cap, c.src)
}

func (c *arc[K, V]) Get(k K) (V, bool) {
	defer c.check()
	if !c.Contains(k) {
		var zero V
		return zero, false
	}
	val := *c.Acquire(c.ctx, k)
	c.Release(k)
	return val, true
}

func (c *arc[K, V]) Add(k K, v V) {
	defer c.check()
	ptr := c.Acquire(c.ctx, k)
	*ptr = v
	c.Release(k)
}

// Tests from hashicorp/golang-lru /////////////////////////////////////////////

func getRand(tb testing.TB, limit int64) int64 {
	out, err := rand.Int(rand.Reader, big.NewInt(limit))
	if err != nil {
		tb.Fatal(err)
	}
	return out.Int64()
}

func BenchmarkARC_Rand(b *testing.B) {
	l, err := NewARC[int64, int64](b, 8192)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	trace := make([]int64, b.N*2)
	for i := 0; i < b.N*2; i++ {
		trace[i] = getRand(b, 32768)
	}

	b.ResetTimer()

	var hit, miss int
	for i := 0; i < 2*b.N; i++ {
		if i%2 == 0 {
			l.Add(trace[i], trace[i])
		} else {
			if _, ok := l.Get(trace[i]); ok {
				hit++
			} else {
				miss++
			}
		}
	}
	b.Logf("hit: %d miss: %d ratio: %f", hit, miss, float64(hit)/float64(miss))
}

func BenchmarkARC_Freq(b *testing.B) {
	l, err := NewARC[int64, int64](b, 8192)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	trace := make([]int64, b.N*2)
	for i := 0; i < b.N*2; i++ {
		if i%2 == 0 {
			trace[i] = getRand(b, 16384)
		} else {
			trace[i] = getRand(b, 32768)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		l.Add(trace[i], trace[i])
	}
	var hit, miss int
	for i := 0; i < b.N; i++ {
		if _, ok := l.Get(trace[i]); ok {
			hit++
		} else {
			miss++
		}
	}
	b.Logf("hit: %d miss: %d ratio: %f", hit, miss, float64(hit)/float64(miss))
}

type arcOp struct {
	Op  uint8  // [0,3)
	Key uint16 // [0,512)
}

func (op *arcOp) UnmarshalBinary(dat []byte) (int, error) {
	*op = arcOp{
		Op:  (dat[0] >> 6) % 3,
		Key: uint16(dat[0]&0b1)<<8 | uint16(dat[1]),
	}
	return 2, nil
}

func (op arcOp) MarshalBinary() ([]byte, error) {
	return []byte{
		(op.Op << 6) | byte(op.Key>>8),
		byte(op.Key),
	}, nil
}

type arcOps []arcOp

func (ops *arcOps) UnmarshalBinary(dat []byte) (int, error) {
	*ops = make(arcOps, len(dat)/2)
	for i := 0; i < len(dat)/2; i++ {
		_, _ = (*ops)[i].UnmarshalBinary(dat[i*2:])
	}
	return len(*ops) * 2, nil
}

func (ops arcOps) MarshalBinary() ([]byte, error) {
	dat := make([]byte, 0, len(ops)*2)
	for _, op := range ops {
		_dat, _ := op.MarshalBinary()
		dat = append(dat, _dat...)
	}
	return dat, nil
}

func FuzzARC(f *testing.F) {
	n := 200000
	seed := make([]byte, n*2)
	_, err := rand.Read(seed)
	require.NoError(f, err)
	f.Add(seed)

	f.Fuzz(func(t *testing.T, dat []byte) {
		var ops arcOps
		_, _ = ops.UnmarshalBinary(dat)
		defer func() {
			if err := derror.PanicToError(recover()); err != nil {
				t.Errorf("%+v", err)
			}
			if t.Failed() && bytes.Equal(dat, seed) {
				SaveFuzz(f, dat)
			}
		}()
		testARC_RandomOps(t, ops)
	})
}

func testARC_RandomOps(t *testing.T, ops []arcOp) {
	size := 128
	l, err := NewARC[int64, int64](t, 128)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for _, op := range ops {
		key := int64(op.Key)
		r := op.Op
		switch r % 3 {
		case 0:
			l.Add(key, key)
		case 1:
			l.Get(key)
		case 2:
			l.Remove(key)
		}

		if l.t1.Len()+l.t2.Len() > size {
			t.Fatalf("bad: t1: %d t2: %d b1: %d b2: %d p: %d",
				l.t1.Len(), l.t2.Len(), l.b1.Len(), l.b2.Len(), l.p())
		}
		if l.b1.Len()+l.b2.Len() > size {
			t.Fatalf("bad: t1: %d t2: %d b1: %d b2: %d p: %d",
				l.t1.Len(), l.t2.Len(), l.b1.Len(), l.b2.Len(), l.p())
		}
	}
}

func TestARC_Get_RecentToFrequent(t *testing.T) {
	t.Parallel()
	l, err := NewARC[int, int](t, 128)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Touch all the entries, should be in t1
	for i := 0; i < 128; i++ {
		l.Add(i, i)
	}
	if n := l.t1.Len(); n != 128 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.t2.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	// Get should upgrade to t2
	for i := 0; i < 128; i++ {
		if _, ok := l.Get(i); !ok {
			t.Fatalf("missing: %d", i)
		}
	}
	if n := l.t1.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.t2.Len(); n != 128 {
		t.Fatalf("bad: %d", n)
	}

	// Get be from t2
	for i := 0; i < 128; i++ {
		if _, ok := l.Get(i); !ok {
			t.Fatalf("missing: %d", i)
		}
	}
	if n := l.t1.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.t2.Len(); n != 128 {
		t.Fatalf("bad: %d", n)
	}
}

func TestARC_Add_RecentToFrequent(t *testing.T) {
	t.Parallel()
	l, err := NewARC[int, int](t, 128)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Add initially to t1
	l.Add(1, 1)
	if n := l.t1.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.t2.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}

	// Add should upgrade to t2
	l.Add(1, 1)
	if n := l.t1.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.t2.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}

	// Add should remain in t2
	l.Add(1, 1)
	if n := l.t1.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.t2.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
}

func TestARC_Adaptive(t *testing.T) {
	t.Parallel()
	l, err := NewARC[int, int](t, 4)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Fill t1
	for i := 0; i < 4; i++ {
		l.Add(i, i)
	}
	require.Equal(t, `[   _0_    _1_    _2_    _3_ !^]___    ___    ___    ___    `, l.String())
	if n := l.t1.Len(); n != 4 {
		t.Fatalf("bad: %d", n)
	}

	// Move to t2
	l.Get(0)
	require.Equal(t, `    ___[   _1_    _2_    _3_ !^ _0_   ]___    ___    ___    `, l.String())
	l.Get(1)
	require.Equal(t, `    ___    ___[   _2_    _3_ !^ _1_    _0_   ]___    ___    `, l.String())
	if n := l.t2.Len(); n != 2 {
		t.Fatalf("bad: %d", n)
	}

	// Evict from t1
	l.Add(4, 4)
	if n := l.b1.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}

	// Current state
	// t1 : (MRU) [4, 3] (LRU)
	// t2 : (MRU) [1, 0] (LRU)
	// b1 : (MRU) [2] (LRU)
	// b2 : (MRU) [] (LRU)
	require.Equal(t, `    ___    _2_[   _3_    _4_ !^ _1_    _0_   ]___    ___    `, l.String())

	// Add 2, should cause hit on b1
	l.Add(2, 2)
	require.Equal(t, `    ___    ___    _3_[ ^ _4_ !  _2_    _1_    _0_   ]___    `, l.String())
	if n := l.b1.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if l.p() != 1 {
		t.Fatalf("bad: %d", l.p())
	}
	if n := l.t2.Len(); n != 3 {
		t.Fatalf("bad: %d", n)
	}

	// Current state
	// t1 : (MRU) [4] (LRU)
	// t2 : (MRU) [2, 1, 0] (LRU)
	// b1 : (MRU) [3] (LRU)
	// b2 : (MRU) [] (LRU)
	require.Equal(t, `    ___    ___    _3_[ ^ _4_ !  _2_    _1_    _0_   ]___    `, l.String())

	// Add 4, should migrate to t2
	l.Add(4, 4)
	require.Equal(t, `    ___    ___    ___  ^ _3_[!  _4_    _2_    _1_    _0_   ]`, l.String())
	if n := l.t1.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.t2.Len(); n != 4 {
		t.Fatalf("bad: %d", n)
	}

	// Current state
	// t1 : (MRU) [] (LRU)
	// t2 : (MRU) [4, 2, 1, 0] (LRU)
	// b1 : (MRU) [3] (LRU)
	// b2 : (MRU) [] (LRU)
	require.Equal(t, `    ___    ___    ___  ^ _3_[!  _4_    _2_    _1_    _0_   ]`, l.String())

	// Add 5, should evict to b2
	l.Add(5, 5)
	require.Equal(t, `    ___    ___    _3_[ ^ _5_ !  _4_    _2_    _1_   ]_0_    `, l.String())
	if n := l.t1.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.t2.Len(); n != 3 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.b2.Len(); n != 1 {
		t.Fatalf("bad: %d", n)
	}

	// Current state
	// t1 : (MRU) [5] (LRU)
	// t2 : (MRU) [4, 2, 1] (LRU)
	// b1 : (MRU) [3] (LRU)
	// b2 : (MRU) [0] (LRU)
	require.Equal(t, `    ___    ___    _3_[ ^ _5_ !  _4_    _2_    _1_   ]_0_    `, l.String())

	// Add 0, should decrease p
	l.Add(0, 0)
	require.Equal(t, `    ___    ___    _3_    _5_[!^ _0_    _4_    _2_    _1_   ]`, l.String())
	if n := l.t1.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.t2.Len(); n != 4 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.b1.Len(); n != 2 {
		t.Fatalf("bad: %d", n)
	}
	if n := l.b2.Len(); n != 0 {
		t.Fatalf("bad: %d", n)
	}
	if l.p() != 0 {
		t.Fatalf("bad: %d", l.p())
	}

	// Current state
	// t1 : (MRU) [] (LRU)
	// t2 : (MRU) [0, 4, 2, 1] (LRU)
	// b1 : (MRU) [5, 3] (LRU)
	// b2 : (MRU) [] (LRU)
	require.Equal(t, `    ___    ___    _3_    _5_[!^ _0_    _4_    _2_    _1_   ]`, l.String())
}

func TestARC(t *testing.T) {
	t.Parallel()
	l, err := NewARC[int, int](t, 128)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 0; i < 256; i++ {
		l.Add(i, i)
	}
	if l.Len() != 128 {
		t.Fatalf("bad len: %v", l.Len())
	}

	for i, k := range l.Keys() {
		if v, ok := l.Get(k); !ok || v != k || v != i+128 {
			t.Fatalf("bad key: %v", k)
		}
	}
	for i := 0; i < 128; i++ {
		if _, ok := l.Get(i); ok {
			t.Fatalf("should be evicted")
		}
	}
	for i := 128; i < 256; i++ {
		if _, ok := l.Get(i); !ok {
			t.Fatalf("should not be evicted")
		}
	}
	for i := 128; i < 192; i++ {
		l.Remove(i)
		if _, ok := l.Get(i); ok {
			t.Fatalf("should be deleted")
		}
	}

	l.Purge()
	if l.Len() != 0 {
		t.Fatalf("bad len: %v", l.Len())
	}
	if _, ok := l.Get(200); ok {
		t.Fatalf("should contain nothing")
	}
}

// Test that Contains doesn't update recent-ness
func TestARC_Contains(t *testing.T) {
	t.Parallel()
	l, err := NewARC[int, int](t, 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	l.Add(1, 1)
	l.Add(2, 2)
	if !l.Contains(1) {
		t.Errorf("1 should be contained")
	}

	l.Add(3, 3)
	if l.Contains(1) {
		t.Errorf("Contains should not have updated recent-ness of 1")
	}
}

// Test that Peek doesn't update recent-ness
func TestARC_Peek(t *testing.T) {
	t.Parallel()
	l, err := NewARC[int, int](t, 2)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	l.Add(1, 1)
	l.Add(2, 2)
	if v, ok := l.Peek(1); !ok || v != 1 {
		t.Errorf("1 should be set to 1: %v, %v", v, ok)
	}

	l.Add(3, 3)
	if l.Contains(1) {
		t.Errorf("should not have updated recent-ness of 1")
	}
}
