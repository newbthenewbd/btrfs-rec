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
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

func reAttachNodes(ctx context.Context, fs _FS, orphanedNodes map[btrfsvol.LogicalAddr]struct{}, rebuiltNodes map[btrfsvol.LogicalAddr]*RebuiltNode) error {
	dlog.Info(ctx, "Attaching orphaned nodes to rebuilt nodes...")

	sb, err := fs.Superblock()
	if err != nil {
		return err
	}

	// Index 'rebuiltNodes' for fast lookups.
	dlog.Info(ctx, "... indexing rebuilt nodes...")
	gaps := make(map[btrfsprim.ObjID]map[uint8][]*RebuiltNode)
	maxLevel := make(map[btrfsprim.ObjID]uint8)
	for _, node := range rebuiltNodes {
		for treeID := range node.InTrees {
			maxLevel[treeID] = slices.Max(maxLevel[treeID], node.Head.Level)
			if gaps[treeID] == nil {
				gaps[treeID] = make(map[uint8][]*RebuiltNode)
			}
			gaps[treeID][node.Head.Level] = append(gaps[treeID][node.Head.Level], node)
		}
	}
	for _, byTreeID := range gaps {
		for _, slice := range byTreeID {
			sort.Slice(slice, func(i, j int) bool {
				return slice[i].MinKey.Cmp(slice[j].MinKey) < 0
			})
		}
	}
	dlog.Info(ctx, "... done indexing")

	// Attach orphanedNodes to the gaps.
	dlog.Info(ctx, "... attaching nodes...")
	lastPct := -1
	progress := func(done int) {
		pct := int(100 * float64(done) / float64(len(orphanedNodes)))
		if pct != lastPct || done == len(orphanedNodes) {
			dlog.Infof(ctx, "... %v%% (%v/%v)",
				pct, done, len(orphanedNodes))
			lastPct = pct
		}
	}
	numAttached := 0
	for i, foundLAddr := range maps.SortedKeys(orphanedNodes) {
		progress(i)
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
			parent.Head.Generation = slices.Max(parent.Head.Generation, foundRef.Data.Head.Generation)
			attached = true
			numAttached++
		}
		if !attached {
			dlog.Errorf(ctx, "could not find a broken node to attach node to reattach node@%v to",
				foundRef.Addr)
		}
	}
	progress(len(orphanedNodes))
	dlog.Info(ctx, "... ... done attaching")

	dlog.Infof(ctx, "... re-attached %d nodes (%v%% success rate)",
		numAttached, int(100*float64(numAttached)/float64(len(orphanedNodes))))
	return nil
}
