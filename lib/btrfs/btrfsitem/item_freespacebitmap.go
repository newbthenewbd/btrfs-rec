// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

// key.objectid = object ID of the FreeSpaceInfo (logical_addr)
// key.offset = offset of the FreeSpaceInfo (size)
type FreeSpaceBitmap struct { // FREE_SPACE_BITMAP=200
	Bitmap []byte
}

func (o *FreeSpaceBitmap) UnmarshalBinary(dat []byte) (int, error) {
	o.Bitmap = dat
	return len(dat), nil
}

func (o FreeSpaceBitmap) MarshalBinary() ([]byte, error) {
	return o.Bitmap, nil
}
