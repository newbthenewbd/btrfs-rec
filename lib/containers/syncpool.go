// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"sync"
)

type SyncPool[T any] struct {
	New func() T

	inner sync.Pool
}

func (p *SyncPool[T]) Get() (val T, ok bool) {
	_val := p.inner.Get()
	switch {
	case _val != nil:
		//nolint:forcetypeassert // Typed wrapper around untyped lib.
		return _val.(T), true
	case p.New != nil:
		return p.New(), true
	default:
		var zero T
		return zero, false
	}
}

func (p *SyncPool[T]) Put(val T) {
	p.inner.Put(val)
}
