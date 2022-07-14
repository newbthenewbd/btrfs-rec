// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package diskio

import (
	"os"
)

type OSFile[A ~int64] struct {
	*os.File
}

var _ File[assertAddr] = (*OSFile[assertAddr])(nil)

func (f *OSFile[A]) Size() A {
	fi, err := f.Stat()
	if err != nil {
		return 0
	}
	return A(fi.Size())
}

func (f *OSFile[A]) ReadAt(dat []byte, paddr A) (int, error) {
	return f.File.ReadAt(dat, int64(paddr))
}

func (f *OSFile[A]) WriteAt(dat []byte, paddr A) (int, error) {
	return f.File.WriteAt(dat, int64(paddr))
}
