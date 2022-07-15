// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsvol

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
)

type (
	PhysicalAddr int64
	LogicalAddr  int64
	AddrDelta    int64
)

func formatAddr(addr int64, f fmt.State, verb rune) {
	switch verb {
	case 'v', 's', 'q':
		str := fmt.Sprintf("%#016x", addr)
		fmt.Fprintf(f, fmtutil.FmtStateString(f, verb), str)
	default:
		fmt.Fprintf(f, fmtutil.FmtStateString(f, verb), addr)
	}
}

func (a PhysicalAddr) Format(f fmt.State, verb rune) { formatAddr(int64(a), f, verb) }
func (a LogicalAddr) Format(f fmt.State, verb rune)  { formatAddr(int64(a), f, verb) }
func (d AddrDelta) Format(f fmt.State, verb rune)    { formatAddr(int64(d), f, verb) }

func (a PhysicalAddr) Sub(b PhysicalAddr) AddrDelta { return AddrDelta(a - b) }
func (a LogicalAddr) Sub(b LogicalAddr) AddrDelta   { return AddrDelta(a - b) }

func (a PhysicalAddr) Add(b AddrDelta) PhysicalAddr { return a + PhysicalAddr(b) }
func (a LogicalAddr) Add(b AddrDelta) LogicalAddr   { return a + LogicalAddr(b) }

type DeviceID uint64

type QualifiedPhysicalAddr struct {
	Dev  DeviceID
	Addr PhysicalAddr
}

func (a QualifiedPhysicalAddr) Add(b AddrDelta) QualifiedPhysicalAddr {
	return QualifiedPhysicalAddr{
		Dev:  a.Dev,
		Addr: a.Addr.Add(b),
	}
}

func (a QualifiedPhysicalAddr) Cmp(b QualifiedPhysicalAddr) int {
	if d := int(a.Dev - b.Dev); d != 0 {
		return d
	}
	return int(a.Addr - b.Addr)
}
