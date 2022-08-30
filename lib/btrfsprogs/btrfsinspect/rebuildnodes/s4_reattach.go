// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"sort"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

func reAttachNodes(ctx context.Context, fs _FS, foundRoots map[btrfsvol.LogicalAddr]struct{}, rebuiltNodes map[btrfsvol.LogicalAddr]*RebuiltNode) error {
	// Index 'rebuiltNodes' for fast lookups.
	gaps := make(map[btrfsprim.ObjID]map[uint8][]*RebuiltNode)
	maxLevel := make(map[btrfsprim.ObjID]uint8)
	for _, node := range rebuiltNodes {
		maxLevel[node.Head.Owner] = slices.Max(maxLevel[node.Head.Owner], node.Head.Level)

		if gaps[node.Head.Owner] == nil {
			gaps[node.Head.Owner] = make(map[uint8][]*RebuiltNode)
		}
		gaps[node.Head.Owner][node.Head.Level] = append(gaps[node.Head.Owner][node.Head.Level], node)
	}
	for _, byTreeID := range gaps {
		for _, slice := range byTreeID {
			sort.Slice(slice, func(i, j int) bool {
				return slice[i].MinKey.Cmp(slice[j].MinKey) < 0
			})
		}
	}

	// Attach foundRoots to the gaps.
	sb, _ := fs.Superblock()
	for foundLAddr := range foundRoots {
		foundRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, foundLAddr, btrfstree.NodeExpectations{
			LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: foundLAddr},
		})
		if foundRef == nil {
			return err
		}
		foundMinKey, ok := foundRef.Data.MinItem()
		if !ok {
			continue
		}
		foundMaxKey, ok := foundRef.Data.MaxItem()
		if !ok {
			continue
		}
		treeGaps := gaps[foundRef.Data.Head.Owner]
		var attached bool
		for level := foundRef.Data.Head.Level + 1; treeGaps != nil && level <= maxLevel[foundRef.Data.Head.Owner] && !attached; level++ {
			parentGen, ok := treeGaps[level]
			if !ok {
				continue
			}
			parentIdx, ok := slices.Search(parentGen, func(parent *RebuiltNode) int {
				switch {
				case foundMinKey.Cmp(parent.MinKey) < 0:
					// 'parent' is too far right
					return -1
				case foundMaxKey.Cmp(parent.MaxKey) > 0:
					// 'parent' is too far left
					return 1
				default:
					// just right
					return 0
				}
			})
			if !ok {
				continue
			}
			parent := parentGen[parentIdx]
			parent.BodyInternal = append(parent.BodyInternal, btrfstree.KeyPointer{
				Key:        foundMinKey,
				BlockPtr:   foundLAddr,
				Generation: foundRef.Data.Head.Generation,
			})
			attached = true
		}
		if !attached {
			dlog.Errorf(ctx, "could not find a broken node to attach node to reattach node@%v to",
				foundRef.Addr)
		}
	}

	return nil
}
