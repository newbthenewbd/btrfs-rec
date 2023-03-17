// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

// FreeSpaceBitmap is used in conjunction with FreeSpaceInfo for
// highly-fragmented blockgroups.
//
// Key:
//
//	key.objectid = object ID of the FreeSpaceInfo (logical_addr)
//	key.offset   = offset of the FreeSpaceInfo (size)
type FreeSpaceBitmap struct { // complex FREE_SPACE_BITMAP=200
	Bitmap []byte
}

func (o *FreeSpaceBitmap) Free() {
	bytePool.Put(o.Bitmap)
	*o = FreeSpaceBitmap{}
	freeSpaceBitmapPool.Put(o)
}

func (o FreeSpaceBitmap) Clone() FreeSpaceBitmap {
	o.Bitmap = cloneBytes(o.Bitmap)
	return o
}

func (o *FreeSpaceBitmap) UnmarshalBinary(dat []byte) (int, error) {
	o.Bitmap = cloneBytes(dat)
	return len(dat), nil
}

func (o FreeSpaceBitmap) MarshalBinary() ([]byte, error) {
	return append([]byte(nil), o.Bitmap...), nil
}
