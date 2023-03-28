// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"context"
	"testing"
	"time"

	"github.com/datawire/dlib/dlog"
	"github.com/stretchr/testify/assert"
)

func TestLRUBlocking(t *testing.T) {
	t.Parallel()
	const tick = time.Second / 2

	ctx := dlog.NewTestContext(t, false)

	cache := NewLRUCache[int, int](4,
		SourceFunc[int, int](func(_ context.Context, k int, v *int) { *v = k * k }))

	assert.Equal(t, 1, *cache.Acquire(ctx, 1))
	assert.Equal(t, 4, *cache.Acquire(ctx, 2))
	assert.Equal(t, 9, *cache.Acquire(ctx, 3))
	assert.Equal(t, 16, *cache.Acquire(ctx, 4))

	ch := make(chan int)
	start := time.Now()
	go func() {
		ch <- *cache.Acquire(ctx, 5)
	}()
	go func() {
		time.Sleep(tick)
		cache.Release(3)
	}()
	result := <-ch
	dur := time.Since(start)
	assert.Equal(t, 25, result)
	assert.Greater(t, dur, tick)
}

//nolint:paralleltest // Can't be parallel because we test testing.AllocsPerRun.
func TestLRUAllocs(t *testing.T) {
	const (
		cacheLen  = 8
		bigNumber = 128
	)

	ctx := dlog.NewTestContext(t, false)

	evictions := 0
	cache := NewLRUCache[int, int](cacheLen, SourceFunc[int, int](func(_ context.Context, k int, v *int) {
		if *v > 0 {
			evictions++
		}
		*v = k
	}))

	i := 1
	store := func() {
		cache.Acquire(ctx, i)
		cache.Release(i)
		i++
	}

	// it should be alloc-free after construction
	assert.Equal(t, float64(0), testing.AllocsPerRun(cacheLen+1, store))
	assert.Equal(t, 2, evictions)
	assert.Equal(t, float64(0), testing.AllocsPerRun(bigNumber, store))
	assert.Equal(t, 3+bigNumber, evictions)
	// check the len
	assert.Equal(t, cacheLen, len(cache.(*lruCache[int, int]).byName))
	assert.Equal(t, cacheLen, cache.(*lruCache[int, int]).evictable.Len)
	cnt := 0
	for entry := cache.(*lruCache[int, int]).evictable.Oldest; entry != nil; entry = entry.Newer {
		cnt++
	}
	assert.Equal(t, cacheLen, cnt)
	cnt = 0
	for entry := cache.(*lruCache[int, int]).evictable.Newest; entry != nil; entry = entry.Older {
		cnt++
	}
	assert.Equal(t, cacheLen, cnt)
	// check contents
	cnt = 0
	for j := i - 1; j > 0; j-- {
		entry, ok := cache.(*lruCache[int, int]).byName[j]
		if cnt < cacheLen {
			if assert.True(t, ok, j) {
				val := entry.Value.val
				assert.Equal(t, j, val, j)
			}
			cnt++
		} else {
			assert.False(t, ok, j)
		}
	}
}
