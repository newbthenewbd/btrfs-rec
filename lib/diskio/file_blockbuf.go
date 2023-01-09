// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package diskio

import (
	"sync"

	"git.lukeshu.com/go/typedsync"

	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

type bufferedBlock struct {
	Dat []byte
	Err error
}

type bufferedFile[A ~int64] struct {
	inner      File[A]
	mu         sync.RWMutex
	blockSize  A
	blockCache containers.ARCache[A, bufferedBlock]
	blockPool  typedsync.Pool[[]byte]
}

var _ File[assertAddr] = (*bufferedFile[assertAddr])(nil)

func NewBufferedFile[A ~int64](file File[A], blockSize A, cacheSize int) *bufferedFile[A] {
	ret := &bufferedFile[A]{
		inner:     file,
		blockSize: blockSize,
		blockCache: containers.ARCache[A, bufferedBlock]{
			MaxLen: cacheSize,
		},
		blockPool: typedsync.Pool[[]byte]{
			New: func() []byte {
				return make([]byte, blockSize)
			},
		},
	}
	ret.blockCache.OnRemove = func(_ A, buf bufferedBlock) {
		ret.blockPool.Put(buf.Dat)
	}
	return ret
}

func (bf *bufferedFile[A]) Name() string { return bf.inner.Name() }
func (bf *bufferedFile[A]) Size() A      { return bf.inner.Size() }
func (bf *bufferedFile[A]) Close() error { return bf.inner.Close() }

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
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	offsetWithinBlock := off % bf.blockSize
	blockOffset := off - offsetWithinBlock
	cachedBlock, ok := bf.blockCache.Load(blockOffset)
	if !ok {
		cachedBlock.Dat, _ = bf.blockPool.Get()
		n, err := bf.inner.ReadAt(cachedBlock.Dat, blockOffset)
		cachedBlock.Dat = cachedBlock.Dat[:n]
		cachedBlock.Err = err
		bf.blockCache.Store(blockOffset, cachedBlock)
	}
	n = copy(dat, cachedBlock.Dat[offsetWithinBlock:])
	if n < len(dat) {
		return n, cachedBlock.Err
	}
	return n, nil
}

func (bf *bufferedFile[A]) WriteAt(dat []byte, off A) (n int, err error) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	// Do the work
	n, err = bf.inner.WriteAt(dat, off)

	// Cache invalidation
	for blockOffset := off - (off % bf.blockSize); blockOffset < off+A(n); blockOffset += bf.blockSize {
		bf.blockCache.Delete(blockOffset)
	}

	return
}
