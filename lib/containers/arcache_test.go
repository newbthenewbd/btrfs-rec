// Copyright (C) 2015, 2022  HashiCorp, Inc.
// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: MPL-2.0
//
// Based on https://github.com/hashicorp/golang-lru/blob/efb1d5b30f66db326f4d8e27b3a5ad04f5e02ca3/arc_test.go

package containers

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/datawire/dlib/derror"
	"github.com/stretchr/testify/require"
)

// Add runtime validity checks /////////////////////////////////////////////////

func (c *ARCache[K, V]) err(e error) error {
	return fmt.Errorf("%[1]T(%[1]p): %w (b1:%v t1:%v / t2:%v b2:%v)",
		c, e,
		c.recentGhost.Len(),
		c.recentLive.Len(),
		c.frequentLive.Len(),
		c.frequentGhost.Len())
}

func (c *ARCache[K, V]) check() {
	if c.MaxLen < 2 {
		panic(c.err(fmt.Errorf("MaxLen:%v < 2", c.MaxLen)))
	}

	if fullLen := c.fullLen(); fullLen > 2*c.MaxLen {
		panic(c.err(fmt.Errorf("fullLen:%v > MaxLen:%v", fullLen, c.MaxLen)))
	}

	if liveLen := c.liveLen(); liveLen > c.MaxLen {
		panic(c.err(fmt.Errorf("liveLen:%v > MaxLen:%v", liveLen, c.MaxLen)))
	}
	if ghostLen := c.ghostLen(); ghostLen > c.MaxLen {
		panic(c.err(fmt.Errorf("ghostLen:%v > MaxLen:%v", ghostLen, c.MaxLen)))
	}
	if recentLen := c.recentLen(); recentLen > c.MaxLen {
		panic(c.err(fmt.Errorf("recentLen:%v > MaxLen:%v", recentLen, c.MaxLen)))
	}
	if frequentLen := c.frequentLen(); frequentLen > c.MaxLen {
		panic(c.err(fmt.Errorf("frequentLen:%v > MaxLen:%v", frequentLen, c.MaxLen)))
	}
}

// Compatibility layer for hashicorp/golang-lru ////////////////////////////////

type arc[K comparable, V any] struct {
	ARCache[K, V]
	t1, t2 *lruCache[K, V]
	b1, b2 *lruCache[K, struct{}]
}

func NewARC[K comparable, V any](size int) (*arc[K, V], error) {
	ret := &arc[K, V]{
		ARCache: ARCache[K, V]{
			MaxLen: size,
		},
	}
	ret.t1 = &ret.recentLive
	ret.t2 = &ret.frequentLive
	ret.b1 = &ret.recentGhost
	ret.b2 = &ret.frequentGhost
	return ret, nil
}

func (c *arc[K, V]) Contains(k K) bool { return c.Has(k) }
func (c *arc[K, V]) Get(k K) (V, bool) { return c.Load(k) }
func (c *arc[K, V]) Add(k K, v V)      { c.Store(k, v) }
func (c *arc[K, V]) Remove(k K)        { c.Delete(k) }
func (c *arc[K, V]) p() int            { return c.recentLiveTarget }
func (c *arc[K, V]) Purge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recentLive = lruCache[K, V]{}
	c.recentGhost = lruCache[K, struct{}]{}
	c.frequentLive = lruCache[K, V]{}
	c.frequentGhost = lruCache[K, struct{}]{}
	c.recentLiveTarget = 0
}

func (c *arc[K, V]) Keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ret := make([]K, 0, c.Len())
	for entry := c.recentLive.byAge.oldest; entry != nil; entry = entry.newer {
		ret = append(ret, entry.Value.key)
	}
	for entry := c.frequentLive.byAge.oldest; entry != nil; entry = entry.newer {
		ret = append(ret, entry.Value.key)
	}
	return ret
}

// Tests from hashicorp/golang-lru /////////////////////////////////////////////

func getRand(tb testing.TB) int64 {
	out, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		tb.Fatal(err)
	}
	return out.Int64()
}

func BenchmarkARC_Rand(b *testing.B) {
	l, err := NewARC[int64, int64](8192)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	trace := make([]int64, b.N*2)
	for i := 0; i < b.N*2; i++ {
		trace[i] = getRand(b) % 32768
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
	l, err := NewARC[int64, int64](8192)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	trace := make([]int64, b.N*2)
	for i := 0; i < b.N*2; i++ {
		if i%2 == 0 {
			trace[i] = getRand(b) % 16384
		} else {
			trace[i] = getRand(b) % 32768
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
	l, err := NewARC[int64, int64](128)
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
	l, err := NewARC[int, int](128)
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
	l, err := NewARC[int, int](128)
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
	l, err := NewARC[int, int](4)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Fill t1
	for i := 0; i < 4; i++ {
		l.Add(i, i)
	}
	if n := l.t1.Len(); n != 4 {
		t.Fatalf("bad: %d", n)
	}

	// Move to t2
	l.Get(0)
	l.Get(1)
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
	l, err := NewARC[int, int](128)
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
	l, err := NewARC[int, int](2)
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
	l, err := NewARC[int, int](2)
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
