// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"context"
	"math"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

type AllSums struct {
	Logical  []btrfssum.SumRun[btrfsvol.LogicalAddr]
	Physical map[btrfsvol.DeviceID]btrfssum.SumRun[btrfsvol.PhysicalAddr]
}

func (as AllSums) SumForPAddr(paddr btrfsvol.QualifiedPhysicalAddr) (btrfssum.ShortSum, bool) {
	run, ok := as.Physical[paddr.Dev]
	if !ok {
		return "", false
	}
	return run.SumForAddr(paddr.Addr)
}

func (as AllSums) RunForLAddr(laddr btrfsvol.LogicalAddr) (btrfssum.SumRun[btrfsvol.LogicalAddr], btrfsvol.LogicalAddr, bool) {
	for _, run := range as.Logical {
		if run.Addr > laddr {
			return btrfssum.SumRun[btrfsvol.LogicalAddr]{}, run.Addr, false
		}
		if run.Addr.Add(run.Size()) <= laddr {
			continue
		}
		return run, 0, true
	}
	return btrfssum.SumRun[btrfsvol.LogicalAddr]{}, math.MaxInt64, false
}

func (as AllSums) SumForLAddr(laddr btrfsvol.LogicalAddr) (btrfssum.ShortSum, bool) {
	run, _, ok := as.RunForLAddr(laddr)
	if !ok {
		return "", false
	}
	return run.SumForAddr(laddr)
}

func (as AllSums) WalkLogical(ctx context.Context, fn func(btrfsvol.LogicalAddr, btrfssum.ShortSum) error) error {
	for _, run := range as.Logical {
		if err := run.Walk(ctx, fn); err != nil {
			return err
		}
	}
	return nil
}
