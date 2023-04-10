// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree

import (
	"context"
	"fmt"
	"math"

	"github.com/datawire/dlib/derror"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

// RawTree implements Tree.
type RawTree struct {
	Forrest RawForrest
	TreeRoot
}

var _ Tree = (*RawTree)(nil)

// TreeWalk implements the 'Tree' interface.
func (tree *RawTree) TreeWalk(ctx context.Context, cbs TreeWalkHandler) {
	sb, err := tree.Forrest.NodeSource.Superblock()
	if err != nil {
		if cbs.BadSuperblock != nil {
			cbs.BadSuperblock(err)
		}
		return
	}
	if tree.RootNode == 0 {
		return
	}
	path := Path{
		PathRoot{
			Forrest:      tree.Forrest,
			TreeID:       tree.ID,
			ToAddr:       tree.RootNode,
			ToGeneration: tree.Generation,
			ToLevel:      tree.Level,
		},
	}
	tree.walk(ctx, *sb, path, cbs)
}

func (tree *RawTree) walk(ctx context.Context, sb Superblock, path Path, cbs TreeWalkHandler) {
	if ctx.Err() != nil {
		return
	}

	// 001
	nodeAddr, nodeExp, ok := path.NodeExpectations(ctx, true) // TODO(lukeshu): Consider whether failing open is the right thing here
	if !ok {
		return
	}
	node, err := tree.Forrest.NodeSource.AcquireNode(ctx, nodeAddr, nodeExp)
	defer tree.Forrest.NodeSource.ReleaseNode(node)
	if ctx.Err() != nil {
		return
	}

	// 002
	switch {
	case err == nil:
		if cbs.Node != nil {
			cbs.Node(path, node)
		}
	default:
		process := cbs.BadNode != nil && cbs.BadNode(path, node, err)
		if !process {
			return
		}
	}
	if ctx.Err() != nil {
		return
	}

	// 003-004
	if node == nil {
		return
	}
	// branch a (interior)
	for i, item := range node.BodyInterior {
		toMaxKey := nodeExp.MaxItem.Val
		if i+1 < len(node.BodyInterior) {
			toMaxKey = node.BodyInterior[i+1].Key.Mm()
		}
		itemPath := append(path, PathKP{
			FromTree: node.Head.Owner,
			FromSlot: i,

			ToAddr:       item.BlockPtr,
			ToGeneration: item.Generation,
			ToMinKey:     item.Key,

			ToMaxKey: toMaxKey,
			ToLevel:  node.Head.Level - 1,
		})
		// 003a
		recurse := cbs.KeyPointer == nil || cbs.KeyPointer(itemPath, item)
		if ctx.Err() != nil {
			return
		}
		// 004a
		if recurse {
			tree.walk(ctx, sb, itemPath, cbs)
			if ctx.Err() != nil {
				return
			}
		}
	}
	// branch b (leaf)
	if cbs.Item == nil && cbs.BadItem == nil {
		return
	}
	for i, item := range node.BodyLeaf {
		itemPath := append(path, PathItem{
			FromTree: node.Head.Owner,
			FromSlot: i,

			ToKey: item.Key,
		})
		// 003b
		switch item.Body.(type) {
		case *btrfsitem.Error:
			if cbs.BadItem != nil {
				cbs.BadItem(itemPath, item)
			}
		default:
			if cbs.Item != nil {
				cbs.Item(itemPath, item)
			}
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// searchKP takes a sorted list of KeyPointers, and finds the
//
//   - left-most member for which `searchFn(member.Key, math.MaxUint32) == 0`; or else the
//   - right-most member for which `searchFn(member.Key, math.MaxUint32) == 1`; or else
//   - zero
//
// This assumes that `haystack` is sorted such that the return values from searchFn look like:
//
//   - + + 0 0 0 - - -
func searchKP(haystack []KeyPointer, searchFn func(key btrfsprim.Key, size uint32) int) (_ KeyPointer, ok bool) {
	if leftZero, ok := slices.SearchLowest(haystack, func(kp KeyPointer) int {
		return searchFn(kp.Key, math.MaxUint32)
	}); ok {
		return haystack[leftZero], true
	}
	if rightPos, ok := slices.SearchHighest(haystack, func(kp KeyPointer) int {
		return slices.Min(searchFn(kp.Key, math.MaxUint32), 0)
	}); ok {
		return haystack[rightPos], true
	}
	return KeyPointer{}, false
}

// TreeSearch implements the 'Tree' interface.
func (tree *RawTree) TreeSearch(ctx context.Context, searcher TreeSearcher) (Item, error) {
	ctx, cancel := context.WithCancel(ctx)
	var retErr error
	setErr := func(err error) {
		if retErr == nil && err != nil {
			retErr = fmt.Errorf("item with %s: %w", searcher, err)
		}
		cancel()
	}

	var ret Item
	var selKP KeyPointer
	tree.TreeWalk(ctx, TreeWalkHandler{
		Node: func(_ Path, node *Node) {
			if node.Head.Level > 0 { // interior node
				kp, ok := searchKP(node.BodyInterior, searcher.Search)
				if !ok {
					setErr(ErrNoItem)
					return
				}
				selKP = kp
			} else { // leaf node
				slot, ok := slices.Search(node.BodyLeaf, func(item Item) int {
					return searcher.Search(item.Key, item.BodySize)
				})
				if !ok {
					setErr(ErrNoItem)
					return
				}
				ret = node.BodyLeaf[slot]
				ret.Body = ret.Body.CloneItem()
			}
		},
		BadNode: func(path Path, _ *Node, err error) bool {
			setErr(fmt.Errorf("%v: %w", path, err))
			return false
		},
		KeyPointer: func(_ Path, kp KeyPointer) bool {
			return kp == selKP
		},
	})

	return ret, retErr
}

// TreeLookup implements the 'Tree' interface.
func (tree *RawTree) TreeLookup(ctx context.Context, key btrfsprim.Key) (Item, error) {
	return tree.TreeSearch(ctx, SearchExactKey(key))
}

// TreeRange implements the 'Tree' interface.
func (tree *RawTree) TreeRange(ctx context.Context, handleFn func(Item) bool) error {
	ctx, cancel := context.WithCancel(ctx)
	var errs derror.MultiError

	tree.TreeWalk(ctx, TreeWalkHandler{
		BadNode: func(path Path, _ *Node, err error) bool {
			errs = append(errs, fmt.Errorf("%v: %w", path, err))
			return false
		},
		Item: func(_ Path, item Item) {
			if !handleFn(item) {
				cancel()
			}
		},
		BadItem: func(_ Path, item Item) {
			if !handleFn(item) {
				cancel()
			}
		},
	})

	if len(errs) > 0 {
		return errs
	}
	return nil
}

// TreeSubrange implements the 'Tree' interface.
func (tree *RawTree) TreeSubrange(ctx context.Context, min int, searcher TreeSearcher, handleFn func(Item) bool) error {
	ctx, cancel := context.WithCancel(ctx)
	var errs derror.MultiError

	var minKP btrfsprim.Key
	var cnt int
	tree.TreeWalk(ctx, TreeWalkHandler{
		Node: func(_ Path, node *Node) {
			// Only bother for interior nodes.
			if node.Head.Level == 0 {
				return
			}
			kp, ok := searchKP(node.BodyInterior, searcher.Search)
			if !ok {
				cancel()
				return
			}
			minKP = kp.Key
		},
		BadNode: func(path Path, _ *Node, err error) bool {
			errs = append(errs, fmt.Errorf("%v: %w", path, err))
			return false
		},
		KeyPointer: func(_ Path, kp KeyPointer) bool {
			if searcher.Search(kp.Key, math.MaxUint32) < 0 {
				cancel()
				return false
			}
			if kp.Key.Compare(minKP) > 0 {
				return false
			}
			return true
		},
		Item: func(_ Path, item Item) {
			d := searcher.Search(item.Key, item.BodySize)
			switch {
			case d > 1:
				// do nothing
			case d == 0:
				cnt++
				if !handleFn(item) {
					cancel()
				}
			case d < 1:
				cancel()
			}
		},
		BadItem: func(_ Path, item Item) {
			d := searcher.Search(item.Key, item.BodySize)
			switch {
			case d > 1:
				// do nothing
			case d == 0:
				cnt++
				if !handleFn(item) {
					cancel()
				}
			case d < 1:
				cancel()
			}
		},
	})

	if cnt < min {
		errs = append(errs, ErrNoItem)
	}
	if len(errs) > 0 {
		return fmt.Errorf("items with %s: %w", searcher, errs)
	}
	return nil
}

// TreeParentID implements the 'Tree' interface.
func (tree *RawTree) TreeParentID(ctx context.Context) (btrfsprim.ObjID, btrfsprim.Generation, error) {
	if tree.ParentUUID == (btrfsprim.UUID{}) {
		return 0, 0, nil
	}
	uuidTree, err := tree.Forrest.RawTree(ctx, btrfsprim.UUID_TREE_OBJECTID)
	if err != nil {
		return 0, 0, err
	}
	parentIDItem, err := uuidTree.TreeLookup(ctx, btrfsitem.UUIDToKey(tree.ParentUUID))
	if err != nil {
		return 0, 0, err
	}
	switch parentIDBody := parentIDItem.Body.(type) {
	case *btrfsitem.UUIDMap:
		return parentIDBody.ObjID, tree.ParentGen, nil
	case *btrfsitem.Error:
		return 0, 0, parentIDBody.Err
	default:
		// This is a panic because the item decoder should not emit UUID_SUBVOL items as anything but
		// btrfsitem.UUIDMap or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: UUID_SUBVOL item has unexpected type: %T", parentIDBody))
	}
}
