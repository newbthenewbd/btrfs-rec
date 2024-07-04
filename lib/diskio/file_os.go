// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package diskio

import (
	"os"
	"io"
)

type OSFile[A ~int64] struct {
	*os.File
}

var _ File[assertAddr] = (*OSFile[assertAddr])(nil)

func (f *OSFile[A]) Size() A {
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0
	}
	return A(size)
}

func (f *OSFile[A]) ReadAt(dat []byte, paddr A) (int, error) {
	return f.File.ReadAt(dat, int64(paddr))
}

func (f *OSFile[A]) WriteAt(dat []byte, paddr A) (int, error) {
	return f.File.WriteAt(dat, int64(paddr))
}
