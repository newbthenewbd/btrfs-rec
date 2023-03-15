// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"sync"
)

// A nestedMutex is like a sync.Mutex, but while it is locked by call
// 'A', may be simultaneously locked by subsequent calls if the
// subsequent calls use a Context descended from the one returned by
// the 'A' call to .Lock().
type nestedMutex struct {
	inner sync.Mutex
	depth int
}

type nestedMutexCtxKey struct{}

// Lock locks the mutex.  It is invalid to use a Context returned from
// Lock in a different goroutine than the one it was created in.  It
// is invalid to use a Context returned from Lock after the mutex has
// subsequently become unlocked.
func (m *nestedMutex) Lock(ctx context.Context) context.Context {
	if other, ok := ctx.Value(nestedMutexCtxKey{}).(*nestedMutex); ok && other == m {
		m.depth++
		return ctx
	}
	m.inner.Lock()
	return context.WithValue(ctx, nestedMutexCtxKey{}, m)
}

// Unlock unlocks the mutex.  It is invalid to call Unlock if the
// mutex is not already locked.  It is invalid to call Unlock from
// multiple goroutines simultaneously.
func (m *nestedMutex) Unlock() {
	if m.depth > 0 {
		m.depth--
	} else {
		m.inner.Unlock()
	}
}
