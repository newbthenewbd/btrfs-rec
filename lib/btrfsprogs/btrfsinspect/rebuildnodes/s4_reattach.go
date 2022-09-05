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
)

func (a RebuiltNode) ContainsWholeRegion(min, max btrfsprim.Key) int {
	switch {
	case min.Cmp(a.MinKey) < 0:
		// 'a' is too far right
		return -1
	case max.Cmp(a.MaxKey) > 0:
		// 'a' is too far left
		return 1
	default:
		// just right
		return 0
	}
}

func reAttachNodes(ctx context.Context, fs _FS, orphanedNodes containers.Set[btrfsvol.LogicalAddr], rebuiltNodes map[btrfsvol.LogicalAddr]*RebuiltNode) error {
	dlog.Info(ctx, "Attaching orphaned nodes to rebuilt nodes...")

	sb, err := fs.Superblock()
	if err != nil {
		return err
	}

	// Index 'rebuiltNodes' for fast lookups.
	dlog.Info(ctx, "... indexing rebuilt nodes...")
	var byLevel [][]*RebuiltNode
	for _, node := range rebuiltNodes {
		for int(node.Head.Level) >= len(byLevel) {
			byLevel = append(byLevel, []*RebuiltNode(nil))
		}
		byLevel[node.Head.Level] = append(byLevel[node.Head.Level], node)
	}
	for _, slice := range byLevel {
		sort.Slice(slice, func(i, j int) bool {
			return slice[i].MinKey.Cmp(slice[j].MinKey) < 0
		})
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

		// `trees` is the set of trees that the node may be
		// placed in; '0' is a wildcard that means "any tree".
		// We still keep track of the others, in order to try
		// to avoid using the wildcard.
		trees := make(containers.Set[btrfsprim.ObjID])
		tree := foundRef.Data.Head.Owner
		for {
			trees.Insert(tree)
			var ok bool
			tree, ok = fs.ParentTree(tree)
			if !ok {
				// error; accept anything
				trees.Insert(0)
				break
			}
			if tree == 0 {
				// end of the line
				break
			}
		}
		attached := make(containers.Set[btrfsprim.ObjID])
		for level := int(foundRef.Data.Head.Level) + 1; level < len(byLevel) && len(attached) == 0; level++ {
			for _, parent := range byLevel[level] {
				if parent.ContainsWholeRegion(foundMinKey, foundMaxKey) != 0 {
					continue
				}
				if parent.Node.Head.Generation < foundRef.Data.Head.Generation {
					continue
				}
				if !trees.HasAny(parent.InTrees) {
					continue
				}
				parent.BodyInternal = append(parent.BodyInternal, btrfstree.KeyPointer{
					Key:        foundMinKey,
					BlockPtr:   foundLAddr,
					Generation: foundRef.Data.Head.Generation,
				})
				attached.InsertFrom(parent.InTrees)
			}
		}
		if _, wildcard := trees[0]; wildcard && len(attached) == 0 {
			for level := int(foundRef.Data.Head.Level) + 1; level < len(byLevel) && len(attached) == 0; level++ {
				for _, parent := range byLevel[level] {
					if parent.ContainsWholeRegion(foundMinKey, foundMaxKey) != 0 {
						continue
					}
					if parent.Node.Head.Generation < foundRef.Data.Head.Generation {
						continue
					}
					parent.BodyInternal = append(parent.BodyInternal, btrfstree.KeyPointer{
						Key:        foundMinKey,
						BlockPtr:   foundLAddr,
						Generation: foundRef.Data.Head.Generation,
					})
					attached.InsertFrom(parent.InTrees)
				}
			}
		}

		if len(attached) > 0 {
			numAttached++
		} else {
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
