// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

//nolint:paralleltest // Can't be parallel because we test testing.AllocsPerRun.
func TestLRU(t *testing.T) {
	const (
		cacheLen  = 8
		bigNumber = 128
	)
	evictions := 0
	cache := &lruCache[int, int]{
		OnEvict: func(_, _ int) {
			evictions++
		},
	}
	i := 0
	store := func() {
		for cache.Len()+1 > cacheLen {
			cache.EvictOldest()
		}
		cache.Store(i, i)
		i++
	}

	// Disable the GC temporarily to prevent cache.byAge.pool from
	// being cleaned in the middle of an AllocsPerRun and causing
	// spurious allocations.
	percent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(percent)

	// 1 alloc each as we fill the cache
	assert.Equal(t, float64(1), testing.AllocsPerRun(cacheLen-1, store))
	assert.Equal(t, 0, evictions)
	// after that, it should be alloc-free
	assert.Equal(t, float64(0), testing.AllocsPerRun(1, store))
	assert.Equal(t, 2, evictions)
	assert.Equal(t, float64(0), testing.AllocsPerRun(bigNumber, store))
	assert.Equal(t, 3+bigNumber, evictions)
	// check the len
	assert.Equal(t, cacheLen, len(cache.byName))
	cnt := 0
	for entry := cache.byAge.newest; entry != nil; entry = entry.older {
		cnt++
	}
	assert.Equal(t, cacheLen, cnt)
	cnt = 0
	for entry := cache.byAge.oldest; entry != nil; entry = entry.newer {
		cnt++
	}
	assert.Equal(t, cacheLen, cnt)
	// check contents
	cnt = 0
	for j := i - 1; j >= 0; j-- {
		if cnt < cacheLen {
			assert.True(t, cache.Has(j), j)
			cnt++
		} else {
			assert.False(t, cache.Has(j), j)
		}
	}
}
