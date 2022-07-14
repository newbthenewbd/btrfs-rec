// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package diskio

import (
	"io"
)

type File[A ~int64] interface {
	Name() string
	Size() A
	Close() error
	ReadAt(p []byte, off A) (n int, err error)
	WriteAt(p []byte, off A) (n int, err error)
}

type assertAddr int64

var (
	_ io.WriterAt = File[int64](nil)
	_ io.ReaderAt = File[int64](nil)
)
