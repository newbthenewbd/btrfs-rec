// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"fmt"
	"sort"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type BlockGroup struct {
	LAddr btrfsvol.LogicalAddr
	Size  btrfsvol.AddrDelta
	Flags btrfsvol.BlockGroupFlags
}

func DedupBlockGroups(scanResults btrfsinspect.ScanDevicesResult) ([]BlockGroup, error) {
	// Dedup
	bgsSet := make(map[BlockGroup]struct{})
	for _, devResults := range scanResults {
		for _, bg := range devResults.FoundBlockGroups {
			bgsSet[BlockGroup{
				LAddr: btrfsvol.LogicalAddr(bg.Key.ObjectID),
				Size:  btrfsvol.AddrDelta(bg.Key.Offset),
				Flags: bg.BG.Flags,
			}] = struct{}{}
		}
	}

	// Sort
	bgsOrdered := maps.Keys(bgsSet)
	sort.Slice(bgsOrdered, func(i, j int) bool {
		return bgsOrdered[i].LAddr < bgsOrdered[j].LAddr
	})

	// Sanity check
	var pos btrfsvol.LogicalAddr
	for _, bg := range bgsOrdered {
		if bg.LAddr < pos || bg.Size < 0 {
			return nil, fmt.Errorf("found block groups are inconsistent")
		}
		pos = bg.LAddr.Add(bg.Size)
	}

	// Return
	return bgsOrdered, nil
}
