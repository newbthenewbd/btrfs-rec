// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"git.lukeshu.com/go/typedsync"
)

type SlicePool[T any] struct {
	// TODO(lukeshu): Consider bucketing slices by size, to
	// increase odds that the `cap(ret) >= size` check passes.
	inner typedsync.Pool[[]T]
}

func (p *SlicePool[T]) Get(size int) []T {
	if size == 0 {
		return nil
	}
	ret, ok := p.inner.Get()
	if ok && cap(ret) >= size {
		ret = ret[:size]
	} else {
		ret = make([]T, size)
	}
	return ret
}

func (p *SlicePool[T]) Put(slice []T) {
	if slice == nil {
		return
	}
	p.inner.Put(slice)
}
