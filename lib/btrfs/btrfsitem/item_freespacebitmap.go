// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

type FreeSpaceBitmap []byte // FREE_SPACE_BITMAP=200

func (o *FreeSpaceBitmap) UnmarshalBinary(dat []byte) (int, error) {
	*o = dat
	return len(dat), nil
}

func (o FreeSpaceBitmap) MarshalBinary() ([]byte, error) {
	return []byte(o), nil
}
