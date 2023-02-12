// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
)

type QGroupStatusFlags uint64

const (
	QGroupStatusFlagOn QGroupStatusFlags = 1 << iota
	QGroupStatusFlagRescan
	QGroupStatusFlagInconsistent
)

var qgroupStatusFlagNames = []string{
	"ON",
	"RESCAN",
	"INCONSISTENT",
}

func (f QGroupStatusFlags) Has(req QGroupStatusFlags) bool { return f&req == req }
func (f QGroupStatusFlags) String() string {
	return fmtutil.BitfieldString(f, qgroupStatusFlagNames, fmtutil.HexNone)
}

const QGroupStatusVersion uint64 = 1

// key.objectid = 0
// key.offset = 0
type QGroupStatus struct { // QGROUP_STATUS=240
	Version        uint64               `bin:"off=0, siz=8"`
	Generation     btrfsprim.Generation `bin:"off=8, siz=8"`
	Flags          QGroupStatusFlags    `bin:"off=16, siz=8"`
	RescanProgress btrfsvol.LogicalAddr `bin:"off=24, siz=8"`
	binstruct.End  `bin:"off=32"`
}
