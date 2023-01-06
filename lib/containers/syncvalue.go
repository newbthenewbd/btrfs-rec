// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"sync"
)

// SyncValue is a typed equivalent of sync/atomic.Value.
//
// It is not actually a wrapper around sync/atomic.Value for
// allocation-performance reasons.
type SyncValue[T comparable] struct {
	mu  sync.Mutex
	ok  bool
	val T
}

// This uses a dumb mutex-based solution because
//
//  1. Performance is good enough, because in the fast-path mutexes
//     use the same compare-and-swap as sync/atomic.Value; and because
//     all of these methods are short we're unlikely to hit the
//     mutex's slow path.
//
//  2. We could use sync/atomic.Pointer[T], which by itself would have
//     the same performance characteristics as sync/atomic.Value but
//     without the benefit of runtime_procPin()/runtime_procUnpin().
//     We want to avoid that because it means we're doing an
//     allocation for every store/swap; avoiding that is our whole
//     reason for not just wraping sync/atomic.Value.  So then we'd
//     want to use a SyncPool to reuse allocations; but (1) that adds
//     more sync-overhead, and (2) it also gets trickier because we'd
//     have to be careful about not adding a pointer back to the pool
//     when load has grabbed the pointer but not yet dereferenced it.

func (v *SyncValue[T]) Load() (val T, ok bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.val, v.ok
}

func (v *SyncValue[T]) Store(val T) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.val, v.ok = val, true
}

func (v *SyncValue[T]) Swap(newV T) (oldV T, oldOK bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	oldV, oldOK = v.val, v.ok
	v.val, v.ok = newV, true
	return
}

func (v *SyncValue[T]) CompareAndSwap(oldV, newV T) (swapped bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.ok || v.val != oldV {
		return false
	}
	v.val = newV
	return true
}
