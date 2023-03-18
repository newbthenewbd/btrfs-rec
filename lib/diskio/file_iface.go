// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package diskio implements utilities for working with disk I/O.
package diskio

import (
	"io"
)

type ReaderAt[A ~int64] interface {
	ReadAt(p []byte, off A) (n int, err error)
}

type File[A ~int64] interface {
	Name() string
	Size() A
	io.Closer
	ReaderAt[A]
	WriteAt(p []byte, off A) (n int, err error)
}

type assertAddr int64

var (
	_ io.WriterAt = File[int64](nil)
	_ io.ReaderAt = File[int64](nil)
)
