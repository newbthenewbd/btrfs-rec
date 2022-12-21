// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"fmt"
	"sort"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type BlockGroup struct {
	LAddr btrfsvol.LogicalAddr
	Size  btrfsvol.AddrDelta
	Flags btrfsvol.BlockGroupFlags
}

func DedupBlockGroups(scanResults btrfsinspect.ScanDevicesResult) (map[btrfsvol.LogicalAddr]BlockGroup, error) {
	// Dedup
	bgsSet := make(containers.Set[BlockGroup])
	for _, devResults := range scanResults {
		for _, bg := range devResults.FoundBlockGroups {
			bgsSet.Insert(BlockGroup{
				LAddr: btrfsvol.LogicalAddr(bg.Key.ObjectID),
				Size:  btrfsvol.AddrDelta(bg.Key.Offset),
				Flags: bg.BG.Flags,
			})
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

	// Return.  We return a map instead of a slice in order to
	// facilitate easy deletes.
	bgsMap := make(map[btrfsvol.LogicalAddr]BlockGroup, len(bgsSet))
	for bg := range bgsSet {
		bgsMap[bg.LAddr] = bg
	}
	return bgsMap, nil
}
