// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
)

type QGroupLimitFlags uint64

const (
	QGroupLimitFlagMaxRfer = 1 << iota
	QGroupLimitFlagMaxExcl
	QGroupLimitFlagRsvRfer
	QGroupLimitFlagRsvExcl
	QGroupLimitFlagRferCmpr
	QGroupLimitFlagExclCmpr
)

var qgroupLimitFlagNames = []string{
	"MAX_RFER",
	"MAX_EXCL",
	"RSV_RFER",
	"RSV_EXCL",
	"RFER_CMPR",
	"EXCL_CMPR",
}

func (f QGroupLimitFlags) Has(req QGroupLimitFlags) bool { return f&req == req }
func (f QGroupLimitFlags) String() string {
	return fmtutil.BitfieldString(f, qgroupLimitFlagNames, fmtutil.HexNone)
}

// key.objectid = 0
// key.offset = ID of the qgroup
type QGroupLimit struct { // trivial QGROUP_LIMIT=244
	Flags         QGroupLimitFlags `bin:"off=0, siz=8"`
	MaxReferenced uint64           `bin:"off=8, siz=8"`
	MaxExclusive  uint64           `bin:"off=16, siz=8"`
	RsvReferenced uint64           `bin:"off=24, siz=8"`
	RsvExclusive  uint64           `bin:"off=32, siz=8"`
	binstruct.End `bin:"off=40"`
}
