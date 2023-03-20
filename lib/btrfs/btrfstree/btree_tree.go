// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"math"

	"github.com/datawire/dlib/derror"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

type RawTree struct {
	Forrest TreeOperatorImpl
	TreeRoot
}

func (tree *RawTree) TreeWalk(ctx context.Context, errHandle func(*TreeError), cbs TreeWalkHandler) {
	path := Path{{
		FromTree:         tree.ID,
		FromItemSlot:     -1,
		ToNodeAddr:       tree.RootNode,
		ToNodeGeneration: tree.Generation,
		ToNodeLevel:      tree.Level,
		ToMaxKey:         btrfsprim.MaxKey,
	}}
	tree.walk(ctx, path, errHandle, cbs)
}

func (tree *RawTree) walk(ctx context.Context, path Path, errHandle func(*TreeError), cbs TreeWalkHandler) {
	if ctx.Err() != nil {
		return
	}
	if path.Node(-1).ToNodeAddr == 0 {
		return
	}

	if cbs.PreNode != nil {
		if err := cbs.PreNode(path); err != nil {
			if errors.Is(err, iofs.SkipDir) {
				return
			}
			errHandle(&TreeError{Path: path, Err: err})
		}
		if ctx.Err() != nil {
			return
		}
	}
	node, err := tree.Forrest.ReadNode(path)
	defer node.Free()
	if ctx.Err() != nil {
		return
	}
	if err != nil && node != nil && cbs.BadNode != nil {
		// opportunity to fix the node
		err = cbs.BadNode(path, node, err)
		if errors.Is(err, iofs.SkipDir) {
			return
		}
	}
	if err != nil {
		errHandle(&TreeError{Path: path, Err: err})
	} else if cbs.Node != nil {
		if err := cbs.Node(path, node); err != nil {
			if errors.Is(err, iofs.SkipDir) {
				return
			}
			errHandle(&TreeError{Path: path, Err: err})
		}
	}
	if ctx.Err() != nil {
		return
	}
	if node != nil {
		for i, item := range node.BodyInterior {
			toMaxKey := path.Node(-1).ToMaxKey
			if i+1 < len(node.BodyInterior) {
				toMaxKey = node.BodyInterior[i+1].Key.Mm()
			}
			itemPath := append(path, PathElem{
				FromTree:         node.Head.Owner,
				FromItemSlot:     i,
				ToNodeAddr:       item.BlockPtr,
				ToNodeGeneration: item.Generation,
				ToNodeLevel:      node.Head.Level - 1,
				ToKey:            item.Key,
				ToMaxKey:         toMaxKey,
			})
			if cbs.PreKeyPointer != nil {
				if err := cbs.PreKeyPointer(itemPath, item); err != nil {
					if errors.Is(err, iofs.SkipDir) {
						continue
					}
					errHandle(&TreeError{Path: itemPath, Err: err})
				}
				if ctx.Err() != nil {
					return
				}
			}
			tree.walk(ctx, itemPath, errHandle, cbs)
			if cbs.PostKeyPointer != nil {
				if err := cbs.PostKeyPointer(itemPath, item); err != nil {
					errHandle(&TreeError{Path: itemPath, Err: err})
				}
				if ctx.Err() != nil {
					return
				}
			}
		}
		for i, item := range node.BodyLeaf {
			itemPath := append(path, PathElem{
				FromTree:     node.Head.Owner,
				FromItemSlot: i,
				ToKey:        item.Key,
				ToMaxKey:     item.Key,
			})
			if errBody, isErr := item.Body.(*btrfsitem.Error); isErr {
				if cbs.BadItem == nil {
					errHandle(&TreeError{Path: itemPath, Err: errBody.Err})
				} else {
					if err := cbs.BadItem(itemPath, item); err != nil {
						errHandle(&TreeError{Path: itemPath, Err: err})
					}
					if ctx.Err() != nil {
						return
					}
				}
			} else {
				if cbs.Item != nil {
					if err := cbs.Item(itemPath, item); err != nil {
						errHandle(&TreeError{Path: itemPath, Err: err})
					}
					if ctx.Err() != nil {
						return
					}
				}
			}
		}
	}
	if cbs.PostNode != nil {
		if err := cbs.PostNode(path, node); err != nil {
			if errors.Is(err, iofs.SkipDir) {
				return
			}
			errHandle(&TreeError{Path: path, Err: err})
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

func (tree *RawTree) TreeSearch(ctx context.Context, searcher TreeSearcher) (Item, error) {
	ctx, cancel := context.WithCancel(ctx)
	var retErr error

	var ret Item
	var selKP KeyPointer
	tree.TreeWalk(ctx, func(err *TreeError) {
		if retErr == nil {
			retErr = fmt.Errorf("item with %s: %w", searcher, err)
		}
		cancel()
	}, TreeWalkHandler{
		Node: func(_ Path, node *Node) error {
			if node.Head.Level > 0 { // interior node
				kp, ok := searchKP(node.BodyInterior, searcher.Search)
				if !ok {
					return ErrNoItem
				}
				selKP = kp
			} else { // leaf node
				slot, ok := slices.Search(node.BodyLeaf, func(item Item) int {
					return searcher.Search(item.Key, item.BodySize)
				})
				if !ok {
					return ErrNoItem
				}
				ret = node.BodyLeaf[slot]
				ret.Body = ret.Body.CloneItem()
			}
			return nil
		},
		PreKeyPointer: func(_ Path, kp KeyPointer) error {
			if kp == selKP {
				return nil
			}
			return iofs.SkipDir
		},
	})

	return ret, retErr
}

func (tree *RawTree) TreeLookup(ctx context.Context, key btrfsprim.Key) (Item, error) {
	return tree.TreeSearch(ctx, SearchExactKey(key))
}

func (tree *RawTree) TreeSubrange(ctx context.Context, min int, searcher TreeSearcher, handleFn func(Item) bool) error {
	ctx, cancel := context.WithCancel(ctx)
	var errs derror.MultiError

	var minKP btrfsprim.Key
	var cnt int
	tree.TreeWalk(ctx, func(err *TreeError) {
		errs = append(errs, err)
	}, TreeWalkHandler{
		Node: func(_ Path, node *Node) error {
			// Only bother for interior nodes.
			if node.Head.Level == 0 {
				return nil
			}
			kp, ok := searchKP(node.BodyInterior, searcher.Search)
			if !ok {
				cancel()
				return nil
			}
			minKP = kp.Key
			return nil
		},
		PreKeyPointer: func(_ Path, kp KeyPointer) error {
			if searcher.Search(kp.Key, math.MaxUint32) < 0 {
				cancel()
				return iofs.SkipDir
			}
			if kp.Key.Compare(minKP) > 0 {
				return iofs.SkipDir
			}
			return nil
		},
		Item: func(_ Path, item Item) error {
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
			return nil
		},
		BadItem: func(_ Path, item Item) error {
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
			return nil
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
