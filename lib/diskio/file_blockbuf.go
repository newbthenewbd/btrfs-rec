// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package diskio

import (
	"context"
	"sync"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/caching"
)

type bufferedBlock[A ~int64] struct {
	Mu    sync.RWMutex
	Addr  A
	Dirty bool

	Dat []byte
	Err error
}

type bufferedFile[A ~int64] struct {
	ctx        context.Context
	inner      File[A]
	blockSize  A
	blockCache caching.Cache[A, bufferedBlock[A]]
}

var _ File[assertAddr] = (*bufferedFile[assertAddr])(nil)

func NewBufferedFile[A ~int64](ctx context.Context, file File[A], blockSize A, cacheSize int) *bufferedFile[A] {
	ret := &bufferedFile[A]{
		inner:     file,
		blockSize: blockSize,
	}
	ret.blockCache = caching.NewLRUCache[A, bufferedBlock[A]](cacheSize, bufferedBlockSource[A]{ret})
	return ret
}

type bufferedBlockSource[A ~int64] struct {
	bf *bufferedFile[A]
}

func (src bufferedBlockSource[A]) Flush(ctx context.Context, block *bufferedBlock[A]) {
	if !block.Dirty {
		return
	}
	if _, err := src.bf.inner.WriteAt(block.Dat, block.Addr); err != nil {
		dlog.Errorf(src.bf.ctx, "i/o error: write: %v", err)
	}
	block.Dirty = false
}

func (src bufferedBlockSource[A]) Load(ctx context.Context, blockAddr A, block *bufferedBlock[A]) {
	src.Flush(ctx, block)
	if block.Dat == nil {
		block.Dat = make([]byte, src.bf.blockSize)
	}
	n, err := src.bf.inner.ReadAt(block.Dat[:src.bf.blockSize], blockAddr)
	block.Addr = blockAddr
	block.Dat = block.Dat[:n]
	block.Err = err
}

func (bf *bufferedFile[A]) Name() string { return bf.inner.Name() }
func (bf *bufferedFile[A]) Size() A      { return bf.inner.Size() }
func (bf *bufferedFile[A]) Close() error { return bf.inner.Close() }

func (bf *bufferedFile[A]) Flush() {
	bf.blockCache.Flush(bf.ctx)
}

func (bf *bufferedFile[A]) ReadAt(dat []byte, off A) (n int, err error) {
	done := 0
	for done < len(dat) {
		n, err := bf.maybeShortReadAt(dat[done:], off+A(done))
		done += n
		if err != nil {
			return done, err
		}
	}
	return done, nil
}

func (bf *bufferedFile[A]) maybeShortReadAt(dat []byte, off A) (n int, err error) {
	offsetWithinBlock := off % bf.blockSize
	blockOffset := off - offsetWithinBlock

	cachedBlock := bf.blockCache.Acquire(bf.ctx, blockOffset)
	defer bf.blockCache.Release(blockOffset)
	cachedBlock.Mu.RLock()
	defer cachedBlock.Mu.RUnlock()

	n = copy(dat, cachedBlock.Dat[offsetWithinBlock:])
	if n < len(dat) {
		return n, cachedBlock.Err
	}
	return n, nil
}

func (bf *bufferedFile[A]) WriteAt(dat []byte, off A) (n int, err error) {
	done := 0
	for done < len(dat) {
		n, err := bf.maybeShortWriteAt(dat[done:], off+A(done))
		done += n
		if err != nil {
			return done, err
		}
	}
	return done, nil
}

func (bf *bufferedFile[A]) maybeShortWriteAt(dat []byte, off A) (n int, err error) {
	offsetWithinBlock := off % bf.blockSize
	blockOffset := off - offsetWithinBlock

	cachedBlock := bf.blockCache.Acquire(bf.ctx, blockOffset)
	defer bf.blockCache.Release(blockOffset)
	cachedBlock.Mu.Lock()
	defer cachedBlock.Mu.Unlock()

	cachedBlock.Dirty = true
	n = copy(cachedBlock.Dat[offsetWithinBlock:], dat)
	if n < len(dat) {
		return n, cachedBlock.Err
	}
	return n, nil
}
