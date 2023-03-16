// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsprim

import (
	"time"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
)

type Generation uint64

type Time struct {
	Sec           int64  `bin:"off=0x0, siz=0x8"` // Number of seconds since 1970-01-01T00:00:00Z.
	NSec          uint32 `bin:"off=0x8, siz=0x4"` // Number of nanoseconds since the beginning of the second.
	binstruct.End `bin:"off=0xc"`
}

func (t Time) ToStd() time.Time {
	return time.Unix(t.Sec, int64(t.NSec))
}
