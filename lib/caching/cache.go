// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package caching

import (
	"context"
)

// A Source is something that a Cache sits in front of.
type Source[K comparable, V any] interface {
	// Load updates a 'V' (which is reused accross the lifetime of
	// the cache, and may or may not be zero) to be set to the
	// value for the 'K'.
	Load(context.Context, K, *V)

	// Flush does whatever it needs to to ensure that if the
	// program exited right now, no one would be upset.  Flush
	// being called does not mean that the entry is being evicted
	// from the cache.
	Flush(context.Context, *V)
}

type Cache[K comparable, V any] interface {
	// Aquire loads the value for `k` (possibly from the cache),
	// records that value in to the cache, and increments the
	// cache entry's in-use counter preventing it from being
	// evicted.
	//
	// If the cache is at capacity and all entries are in-use,
	// then Aquire blocks until an entry becomes available (via
	// `Release`).
	Acquire(context.Context, K) *V

	// Release decrements the in-use counter for the cache entry
	// for `k`.  If the in-use counter drops to 0, then that entry
	// may be evicted.
	//
	// It is invalid (runtime-panic) to call Release for an entry
	// that does not have a positive in-use counter.
	Release(K)

	// Delete invalidates/removes an entry from the cache.  Blocks
	// until the in-user counter drops to 0.
	//
	// It is valid to call Delete on an entry that does not exist
	// in the cache.
	Delete(K)

	// Flush does whatever it needs to to ensure that if the
	// program exited right now, no one would be upset.  Flush
	// does not empty the cache.
	Flush(context.Context)
}

// FuncSource implements Source.  Load calls the function, and Flush
// is a no-op.
type FuncSource[K comparable, V any] func(context.Context, K, *V)

var _ Source[int, string] = FuncSource[int, string](nil)

func (fn FuncSource[K, V]) Load(ctx context.Context, k K, v *V) { fn(ctx, k, v) }
func (fn FuncSource[K, V]) Flush(context.Context, *V)           {}
