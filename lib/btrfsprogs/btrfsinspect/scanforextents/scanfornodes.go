// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package scanforextents

import (
	"encoding/json"
	"fmt"
	"os"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

type NodeScanResults = map[btrfsvol.DeviceID]btrfsinspect.ScanOneDevResult

func ReadNodeScanResults(fs *btrfs.FS, filename string) (*BlockGroupTree, error) {
	scanResultsBytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var scanResults NodeScanResults
	if err := json.Unmarshal(scanResultsBytes, &scanResults); err != nil {
		return nil, err
	}

	bgTree, err := ReduceScanResults(fs, scanResults)
	if err != nil {
		return nil, err
	}

	return bgTree, nil
}

type BlockGroup struct {
	LAddr btrfsvol.LogicalAddr
	Size  btrfsvol.AddrDelta
	Flags btrfsvol.BlockGroupFlags
}

type BlockGroupTree = containers.RBTree[containers.NativeOrdered[btrfsvol.LogicalAddr], BlockGroup]

func LookupBlockGroup(tree *BlockGroupTree, laddr btrfsvol.LogicalAddr, size btrfsvol.AddrDelta) *BlockGroup {
	a := struct {
		LAddr btrfsvol.LogicalAddr
		Size  btrfsvol.AddrDelta
	}{
		LAddr: laddr,
		Size:  size,
	}
	node := tree.Search(func(b BlockGroup) int {
		switch {
		case a.LAddr.Add(a.Size) <= b.LAddr:
			// 'a' is wholly to the left of 'b'.
			return -1
		case b.LAddr.Add(b.Size) <= a.LAddr:
			// 'a' is wholly to the right of 'b'.
			return 1
		default:
			// There is some overlap.
			return 0
		}
	})
	if node == nil {
		return nil
	}
	bg := node.Value
	return &bg
}

func ReduceScanResults(fs *btrfs.FS, scanResults NodeScanResults) (*BlockGroupTree, error) {
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
	bgTree := &BlockGroupTree{
		KeyFn: func(bg BlockGroup) containers.NativeOrdered[btrfsvol.LogicalAddr] {
			return containers.NativeOrdered[btrfsvol.LogicalAddr]{Val: bg.LAddr}
		},
	}
	for bg := range bgSet {
		if laddr, _ := fs.LV.ResolveAny(bg.LAddr, bg.Size); laddr >= 0 {
			continue
		}
		if LookupBlockGroup(bgTree, bg.LAddr, bg.Size) != nil {
			return nil, fmt.Errorf("found block groups are inconsistent")
		}
		bgTree.Insert(bg)
	}
	return bgTree, nil
}
