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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

type TreeOperatorImpl struct {
	NodeSource
}

// TreeWalk implements the 'TreeOperator' interface.
func (fs TreeOperatorImpl) TreeWalk(ctx context.Context, treeID btrfsprim.ObjID, errHandle func(*TreeError), cbs TreeWalkHandler) {
	sb, err := fs.Superblock()
	if err != nil {
		errHandle(&TreeError{Path: TreePath{{FromTree: treeID, ToMaxKey: btrfsprim.MaxKey}}, Err: err})
	}
	rootInfo, err := LookupTreeRoot(fs, *sb, treeID)
	if err != nil {
		errHandle(&TreeError{Path: TreePath{{FromTree: treeID, ToMaxKey: btrfsprim.MaxKey}}, Err: err})
		return
	}
	fs.RawTreeWalk(ctx, *rootInfo, errHandle, cbs)
}

// RawTreeWalk is a utility method to help with implementing the
// 'TreeOperator' interface.
func (fs TreeOperatorImpl) RawTreeWalk(ctx context.Context, rootInfo TreeRoot, errHandle func(*TreeError), cbs TreeWalkHandler) {
	path := TreePath{{
		FromTree:         rootInfo.TreeID,
		FromItemSlot:     -1,
		ToNodeAddr:       rootInfo.RootNode,
		ToNodeGeneration: rootInfo.Generation,
		ToNodeLevel:      rootInfo.Level,
		ToMaxKey:         btrfsprim.MaxKey,
	}}
	fs.treeWalk(ctx, path, errHandle, cbs)
}

func (fs TreeOperatorImpl) treeWalk(ctx context.Context, path TreePath, errHandle func(*TreeError), cbs TreeWalkHandler) {
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
	node, err := fs.ReadNode(path)
	defer FreeNodeRef(node)
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
		for i, item := range node.Data.BodyInterior {
			toMaxKey := path.Node(-1).ToMaxKey
			if i+1 < len(node.Data.BodyInterior) {
				toMaxKey = node.Data.BodyInterior[i+1].Key.Mm()
			}
			itemPath := append(path, TreePathElem{
				FromTree:         node.Data.Head.Owner,
				FromItemSlot:     i,
				ToNodeAddr:       item.BlockPtr,
				ToNodeGeneration: item.Generation,
				ToNodeLevel:      node.Data.Head.Level - 1,
				ToKey:            item.Key,
				ToMaxKey:         toMaxKey,
			})
			if cbs.PreKeyPointer != nil {
				if err := cbs.PreKeyPointer(itemPath, item); err != nil {
					errHandle(&TreeError{Path: itemPath, Err: err})
				}
				if ctx.Err() != nil {
					return
				}
			}
			fs.treeWalk(ctx, itemPath, errHandle, cbs)
			if cbs.PostKeyPointer != nil {
				if err := cbs.PostKeyPointer(itemPath, item); err != nil {
					errHandle(&TreeError{Path: itemPath, Err: err})
				}
				if ctx.Err() != nil {
					return
				}
			}
		}
		for i, item := range node.Data.BodyLeaf {
			itemPath := append(path, TreePathElem{
				FromTree:     node.Data.Head.Owner,
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

func (fs TreeOperatorImpl) treeSearch(treeRoot TreeRoot, fn func(btrfsprim.Key, uint32) int) (TreePath, *diskio.Ref[btrfsvol.LogicalAddr, Node], error) {
	path := TreePath{{
		FromTree:         treeRoot.TreeID,
		FromItemSlot:     -1,
		ToNodeAddr:       treeRoot.RootNode,
		ToNodeGeneration: treeRoot.Generation,
		ToNodeLevel:      treeRoot.Level,
		ToMaxKey:         btrfsprim.MaxKey,
	}}
	for {
		if path.Node(-1).ToNodeAddr == 0 {
			return nil, nil, iofs.ErrNotExist
		}
		node, err := fs.ReadNode(path)
		if err != nil {
			FreeNodeRef(node)
			return nil, nil, err
		}

		if node.Data.Head.Level > 0 {
			// interior node

			// Search for the right-most node.Data.BodyInterior item for which
			// `fn(item.Key) >= 0`.
			//
			//    + + + + 0 - - - -
			//
			// There may or may not be a value that returns '0'.
			//
			// i.e. find the highest value that isn't too high.
			lastGood, ok := slices.SearchHighest(node.Data.BodyInterior, func(kp KeyPointer) int {
				return slices.Min(fn(kp.Key, math.MaxUint32), 0) // don't return >0; a key can't be "too low"
			})
			if !ok {
				FreeNodeRef(node)
				return nil, nil, iofs.ErrNotExist
			}
			toMaxKey := path.Node(-1).ToMaxKey
			if lastGood+1 < len(node.Data.BodyInterior) {
				toMaxKey = node.Data.BodyInterior[lastGood+1].Key.Mm()
			}
			path = append(path, TreePathElem{
				FromTree:         node.Data.Head.Owner,
				FromItemSlot:     lastGood,
				ToNodeAddr:       node.Data.BodyInterior[lastGood].BlockPtr,
				ToNodeGeneration: node.Data.BodyInterior[lastGood].Generation,
				ToNodeLevel:      node.Data.Head.Level - 1,
				ToKey:            node.Data.BodyInterior[lastGood].Key,
				ToMaxKey:         toMaxKey,
			})
			FreeNodeRef(node)
		} else {
			// leaf node

			// Search for a member of node.Data.BodyLeaf for which
			// `fn(item.Head.Key) == 0`.
			//
			//    + + + + 0 - - - -
			//
			// Such an item might not exist; in this case, return nil/ErrNotExist.
			// Multiple such items might exist; in this case, it does not matter which
			// is returned.
			//
			// Implement this search as a binary search.
			slot, ok := slices.Search(node.Data.BodyLeaf, func(item Item) int {
				return fn(item.Key, item.BodySize)
			})
			if !ok {
				FreeNodeRef(node)
				return nil, nil, iofs.ErrNotExist
			}
			path = append(path, TreePathElem{
				FromTree:     node.Data.Head.Owner,
				FromItemSlot: slot,
				ToKey:        node.Data.BodyLeaf[slot].Key,
				ToMaxKey:     node.Data.BodyLeaf[slot].Key,
			})
			return path, node, nil
		}
	}
}

func (fs TreeOperatorImpl) prev(path TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, Node]) (TreePath, *diskio.Ref[btrfsvol.LogicalAddr, Node], error) {
	var err error
	path = path.DeepCopy()

	// go up
	for path.Node(-1).FromItemSlot < 1 {
		path = path.Parent()
		if len(path) == 0 {
			return nil, nil, nil
		}
	}
	// go left
	path.Node(-1).FromItemSlot--
	if path.Node(-1).ToNodeAddr != 0 {
		if node.Addr != path.Node(-2).ToNodeAddr {
			FreeNodeRef(node)
			node, err = fs.ReadNode(path.Parent())
			if err != nil {
				FreeNodeRef(node)
				return nil, nil, err
			}
			path.Node(-1).ToNodeAddr = node.Data.BodyInterior[path.Node(-1).FromItemSlot].BlockPtr
		}
	}
	// go down
	for path.Node(-1).ToNodeAddr != 0 {
		if node.Addr != path.Node(-1).ToNodeAddr {
			FreeNodeRef(node)
			node, err = fs.ReadNode(path)
			if err != nil {
				FreeNodeRef(node)
				return nil, nil, err
			}
		}
		if node.Data.Head.Level > 0 {
			path = append(path, TreePathElem{
				FromTree:         node.Data.Head.Owner,
				FromItemSlot:     len(node.Data.BodyInterior) - 1,
				ToNodeAddr:       node.Data.BodyInterior[len(node.Data.BodyInterior)-1].BlockPtr,
				ToNodeGeneration: node.Data.BodyInterior[len(node.Data.BodyInterior)-1].Generation,
				ToNodeLevel:      node.Data.Head.Level - 1,
				ToKey:            node.Data.BodyInterior[len(node.Data.BodyInterior)-1].Key,
				ToMaxKey:         path.Node(-1).ToMaxKey,
			})
		} else {
			path = append(path, TreePathElem{
				FromTree:     node.Data.Head.Owner,
				FromItemSlot: len(node.Data.BodyLeaf) - 1,
				ToKey:        node.Data.BodyLeaf[len(node.Data.BodyLeaf)-1].Key,
				ToMaxKey:     node.Data.BodyLeaf[len(node.Data.BodyLeaf)-1].Key,
			})
		}
	}
	// return
	if node.Addr != path.Node(-2).ToNodeAddr {
		FreeNodeRef(node)
		node, err = fs.ReadNode(path.Parent())
		if err != nil {
			FreeNodeRef(node)
			return nil, nil, err
		}
	}
	return path, node, nil
}

func (fs TreeOperatorImpl) next(path TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, Node]) (TreePath, *diskio.Ref[btrfsvol.LogicalAddr, Node], error) {
	var err error
	path = path.DeepCopy()

	// go up
	if node.Addr != path.Node(-2).ToNodeAddr {
		FreeNodeRef(node)
		node, err = fs.ReadNode(path.Parent())
		if err != nil {
			FreeNodeRef(node)
			return nil, nil, err
		}
		path.Node(-2).ToNodeLevel = node.Data.Head.Level
	}
	for path.Node(-1).FromItemSlot+1 >= int(node.Data.Head.NumItems) {
		path = path.Parent()
		if len(path) == 1 {
			return nil, nil, nil
		}
		if node.Addr != path.Node(-2).ToNodeAddr {
			FreeNodeRef(node)
			node, err = fs.ReadNode(path.Parent())
			if err != nil {
				FreeNodeRef(node)
				return nil, nil, err
			}
			path.Node(-2).ToNodeLevel = node.Data.Head.Level
		}
	}
	// go right
	path.Node(-1).FromItemSlot++
	if path.Node(-1).ToNodeAddr != 0 {
		if node.Addr != path.Node(-2).ToNodeAddr {
			FreeNodeRef(node)
			node, err = fs.ReadNode(path.Parent())
			if err != nil {
				FreeNodeRef(node)
				return nil, nil, err
			}
			path.Node(-1).ToNodeAddr = node.Data.BodyInterior[path.Node(-1).FromItemSlot].BlockPtr
		}
	}
	// go down
	for path.Node(-1).ToNodeAddr != 0 {
		if node.Addr != path.Node(-1).ToNodeAddr {
			FreeNodeRef(node)
			node, err = fs.ReadNode(path)
			if err != nil {
				FreeNodeRef(node)
				return nil, nil, err
			}
			path.Node(-1).ToNodeLevel = node.Data.Head.Level
		}
		if node.Data.Head.Level > 0 {
			toMaxKey := path.Node(-1).ToMaxKey
			if len(node.Data.BodyInterior) > 1 {
				toMaxKey = node.Data.BodyInterior[1].Key.Mm()
			}
			path = append(path, TreePathElem{
				FromTree:         node.Data.Head.Owner,
				FromItemSlot:     0,
				ToNodeAddr:       node.Data.BodyInterior[0].BlockPtr,
				ToNodeGeneration: node.Data.BodyInterior[0].Generation,
				ToNodeLevel:      node.Data.Head.Level - 1,
				ToKey:            node.Data.BodyInterior[0].Key,
				ToMaxKey:         toMaxKey,
			})
		} else {
			path = append(path, TreePathElem{
				FromTree:     node.Data.Head.Owner,
				FromItemSlot: 0,
				ToKey:        node.Data.BodyInterior[0].Key,
				ToMaxKey:     node.Data.BodyInterior[0].Key,
			})
		}
	}
	// return
	if node.Addr != path.Node(-2).ToNodeAddr {
		FreeNodeRef(node)
		node, err = fs.ReadNode(path.Parent())
		if err != nil {
			FreeNodeRef(node)
			return nil, nil, err
		}
	}
	return path, node, nil
}

// TreeSearch implements the 'TreeOperator' interface.
func (fs TreeOperatorImpl) TreeSearch(treeID btrfsprim.ObjID, fn func(btrfsprim.Key, uint32) int) (Item, error) {
	sb, err := fs.Superblock()
	if err != nil {
		return Item{}, err
	}
	rootInfo, err := LookupTreeRoot(fs, *sb, treeID)
	if err != nil {
		return Item{}, err
	}
	path, node, err := fs.treeSearch(*rootInfo, fn)
	if err != nil {
		return Item{}, err
	}
	item := node.Data.BodyLeaf[path.Node(-1).FromItemSlot]
	item.Body = item.Body.CloneItem()
	FreeNodeRef(node)
	return item, nil
}

// KeySearch returns a comparator suitable to be passed to TreeSearch.
func KeySearch(fn func(btrfsprim.Key) int) func(btrfsprim.Key, uint32) int {
	return func(key btrfsprim.Key, _ uint32) int {
		return fn(key)
	}
}

// TreeLookup implements the 'TreeOperator' interface.
func (fs TreeOperatorImpl) TreeLookup(treeID btrfsprim.ObjID, key btrfsprim.Key) (Item, error) {
	item, err := fs.TreeSearch(treeID, KeySearch(key.Compare))
	if err != nil {
		err = fmt.Errorf("item with key=%v: %w", key, err)
	}
	return item, err
}

// TreeSearchAll implements the 'TreeOperator' interface.
func (fs TreeOperatorImpl) TreeSearchAll(treeID btrfsprim.ObjID, fn func(btrfsprim.Key, uint32) int) ([]Item, error) {
	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}
	rootInfo, err := LookupTreeRoot(fs, *sb, treeID)
	if err != nil {
		return nil, err
	}
	middlePath, middleNode, err := fs.treeSearch(*rootInfo, fn)
	if err != nil {
		return nil, err
	}
	middleItem := middleNode.Data.BodyLeaf[middlePath.Node(-1).FromItemSlot]

	ret := []Item{middleItem}
	var errs derror.MultiError
	prevPath, prevNode := middlePath, middleNode
	for {
		prevPath, prevNode, err = fs.prev(prevPath, prevNode)
		if err != nil {
			errs = append(errs, err)
			break
		}
		if len(prevPath) == 0 {
			break
		}
		prevItem := prevNode.Data.BodyLeaf[prevPath.Node(-1).FromItemSlot]
		if fn(prevItem.Key, prevItem.BodySize) != 0 {
			break
		}
		item := prevItem
		item.Body = item.Body.CloneItem()
		ret = append(ret, item)
	}
	slices.Reverse(ret)
	if prevNode.Addr != middlePath.Node(-1).ToNodeAddr {
		FreeNodeRef(prevNode)
		middleNode, err = fs.ReadNode(middlePath)
		if err != nil {
			FreeNodeRef(middleNode)
			return nil, err
		}
	}
	nextPath, nextNode := middlePath, middleNode
	for {
		nextPath, nextNode, err = fs.next(nextPath, nextNode)
		if err != nil {
			errs = append(errs, err)
			break
		}
		if len(nextPath) == 0 {
			break
		}
		nextItem := nextNode.Data.BodyLeaf[nextPath.Node(-1).FromItemSlot]
		if fn(nextItem.Key, nextItem.BodySize) != 0 {
			break
		}
		item := nextItem
		item.Body = item.Body.CloneItem()
		ret = append(ret, item)
	}
	FreeNodeRef(nextNode)
	if errs != nil {
		err = errs
	}
	return ret, err
}
