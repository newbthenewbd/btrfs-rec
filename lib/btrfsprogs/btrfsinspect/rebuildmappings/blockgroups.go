// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"fmt"
	"sort"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type BlockGroup struct {
	LAddr btrfsvol.LogicalAddr
	Size  btrfsvol.AddrDelta
	Flags btrfsvol.BlockGroupFlags
}

func ReduceScanResults(fs *btrfs.FS, scanResults btrfsinspect.ScanDevicesResult) (map[btrfsvol.LogicalAddr]BlockGroup, error) {
	// Reduce
	bgSet := make(map[BlockGroup]struct{})
	for _, found := range scanResults {
		for _, bg := range found.FoundBlockGroups {
			bgSet[BlockGroup{
				LAddr: btrfsvol.LogicalAddr(bg.Key.ObjectID),
				Size:  btrfsvol.AddrDelta(bg.Key.Offset),
				Flags: bg.BG.Flags,
			}] = struct{}{}
		}
	}

	// Sanity check
	bgList := maps.Keys(bgSet)
	sort.Slice(bgList, func(i, j int) bool {
		return bgList[i].LAddr < bgList[j].LAddr
	})
	var pos btrfsvol.LogicalAddr
	for _, bg := range bgList {
		if bg.LAddr < pos || bg.Size < 0 {
			return nil, fmt.Errorf("found block groups are inconsistent")
		}
		pos = bg.LAddr.Add(bg.Size)
	}

	// Return
	bgMap := make(map[btrfsvol.LogicalAddr]BlockGroup, len(bgSet))
	for bg := range bgSet {
		bgMap[bg.LAddr] = bg
	}
	return bgMap, nil
}
