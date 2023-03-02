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

func (tree *RawTree) search(_ context.Context, fn func(btrfsprim.Key, uint32) int) (Path, *Node, error) {
	path := Path{{
		FromTree:         tree.ID,
		FromItemSlot:     -1,
		ToNodeAddr:       tree.RootNode,
		ToNodeGeneration: tree.Generation,
		ToNodeLevel:      tree.Level,
		ToMaxKey:         btrfsprim.MaxKey,
	}}
	for {
		if path.Node(-1).ToNodeAddr == 0 {
			return nil, nil, ErrNoItem
		}
		node, err := tree.Forrest.ReadNode(path)
		if err != nil {
			node.Free()
			return nil, nil, err
		}

		switch {
		case node.Head.Level > 0:
			// interior node

			// Search for the right-most node.BodyInterior item for which
			// `fn(item.Key) >= 0`.
			//
			//    + + + + 0 - - - -
			//
			// There may or may not be a value that returns '0'.
			//
			// i.e. find the highest value that isn't too high.
			lastGood, ok := slices.SearchHighest(node.BodyInterior, func(kp KeyPointer) int {
				return slices.Min(fn(kp.Key, math.MaxUint32), 0) // don't return >0; a key can't be "too low"
			})
			if !ok {
				node.Free()
				return nil, nil, ErrNoItem
			}
			toMaxKey := path.Node(-1).ToMaxKey
			if lastGood+1 < len(node.BodyInterior) {
				toMaxKey = node.BodyInterior[lastGood+1].Key.Mm()
			}
			path = append(path, PathElem{
				FromTree:         node.Head.Owner,
				FromItemSlot:     lastGood,
				ToNodeAddr:       node.BodyInterior[lastGood].BlockPtr,
				ToNodeGeneration: node.BodyInterior[lastGood].Generation,
				ToNodeLevel:      node.Head.Level - 1,
				ToKey:            node.BodyInterior[lastGood].Key,
				ToMaxKey:         toMaxKey,
			})
			node.Free()
		default:
			// leaf node

			// Search for a member of node.BodyLeaf for which
			// `fn(item.Head.Key) == 0`.
			//
			//    + + + + 0 - - - -
			//
			// Such an item might not exist; in this case, return (nil, ErrNoItem).
			// Multiple such items might exist; in this case, it does not matter which
			// is returned.
			//
			// Implement this search as a binary search.
			slot, ok := slices.Search(node.BodyLeaf, func(item Item) int {
				return fn(item.Key, item.BodySize)
			})
			if !ok {
				node.Free()
				return nil, nil, ErrNoItem
			}
			path = append(path, PathElem{
				FromTree:     node.Head.Owner,
				FromItemSlot: slot,
				ToKey:        node.BodyLeaf[slot].Key,
				ToMaxKey:     node.BodyLeaf[slot].Key,
			})
			return path, node, nil
		}
	}
}

func (fs TreeOperatorImpl) prev(path Path, node *Node) (Path, *Node, error) {
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
		if node.Head.Addr != path.Node(-2).ToNodeAddr {
			node.Free()
			node, err = fs.ReadNode(path.Parent())
			if err != nil {
				node.Free()
				return nil, nil, err
			}
			path.Node(-1).ToNodeAddr = node.BodyInterior[path.Node(-1).FromItemSlot].BlockPtr
		}
	}
	// go down
	for path.Node(-1).ToNodeAddr != 0 {
		if node.Head.Addr != path.Node(-1).ToNodeAddr {
			node.Free()
			node, err = fs.ReadNode(path)
			if err != nil {
				node.Free()
				return nil, nil, err
			}
		}
		if node.Head.Level > 0 {
			path = append(path, PathElem{
				FromTree:         node.Head.Owner,
				FromItemSlot:     len(node.BodyInterior) - 1,
				ToNodeAddr:       node.BodyInterior[len(node.BodyInterior)-1].BlockPtr,
				ToNodeGeneration: node.BodyInterior[len(node.BodyInterior)-1].Generation,
				ToNodeLevel:      node.Head.Level - 1,
				ToKey:            node.BodyInterior[len(node.BodyInterior)-1].Key,
				ToMaxKey:         path.Node(-1).ToMaxKey,
			})
		} else {
			path = append(path, PathElem{
				FromTree:     node.Head.Owner,
				FromItemSlot: len(node.BodyLeaf) - 1,
				ToKey:        node.BodyLeaf[len(node.BodyLeaf)-1].Key,
				ToMaxKey:     node.BodyLeaf[len(node.BodyLeaf)-1].Key,
			})
		}
	}
	// return
	if node.Head.Addr != path.Node(-2).ToNodeAddr {
		node.Free()
		node, err = fs.ReadNode(path.Parent())
		if err != nil {
			node.Free()
			return nil, nil, err
		}
	}
	return path, node, nil
}

func (fs TreeOperatorImpl) next(path Path, node *Node) (Path, *Node, error) {
	var err error
	path = path.DeepCopy()

	// go up
	if node.Head.Addr != path.Node(-2).ToNodeAddr {
		node.Free()
		node, err = fs.ReadNode(path.Parent())
		if err != nil {
			node.Free()
			return nil, nil, err
		}
		path.Node(-2).ToNodeLevel = node.Head.Level
	}
	for path.Node(-1).FromItemSlot+1 >= int(node.Head.NumItems) {
		path = path.Parent()
		if len(path) == 1 {
			return nil, nil, nil
		}
		if node.Head.Addr != path.Node(-2).ToNodeAddr {
			node.Free()
			node, err = fs.ReadNode(path.Parent())
			if err != nil {
				node.Free()
				return nil, nil, err
			}
			path.Node(-2).ToNodeLevel = node.Head.Level
		}
	}
	// go right
	path.Node(-1).FromItemSlot++
	if path.Node(-1).ToNodeAddr != 0 {
		if node.Head.Addr != path.Node(-2).ToNodeAddr {
			node.Free()
			node, err = fs.ReadNode(path.Parent())
			if err != nil {
				node.Free()
				return nil, nil, err
			}
			path.Node(-1).ToNodeAddr = node.BodyInterior[path.Node(-1).FromItemSlot].BlockPtr
		}
	}
	// go down
	for path.Node(-1).ToNodeAddr != 0 {
		if node.Head.Addr != path.Node(-1).ToNodeAddr {
			node.Free()
			node, err = fs.ReadNode(path)
			if err != nil {
				node.Free()
				return nil, nil, err
			}
			path.Node(-1).ToNodeLevel = node.Head.Level
		}
		if node.Head.Level > 0 {
			toMaxKey := path.Node(-1).ToMaxKey
			if len(node.BodyInterior) > 1 {
				toMaxKey = node.BodyInterior[1].Key.Mm()
			}
			path = append(path, PathElem{
				FromTree:         node.Head.Owner,
				FromItemSlot:     0,
				ToNodeAddr:       node.BodyInterior[0].BlockPtr,
				ToNodeGeneration: node.BodyInterior[0].Generation,
				ToNodeLevel:      node.Head.Level - 1,
				ToKey:            node.BodyInterior[0].Key,
				ToMaxKey:         toMaxKey,
			})
		} else {
			path = append(path, PathElem{
				FromTree:     node.Head.Owner,
				FromItemSlot: 0,
				ToKey:        node.BodyInterior[0].Key,
				ToMaxKey:     node.BodyInterior[0].Key,
			})
		}
	}
	// return
	if node.Head.Addr != path.Node(-2).ToNodeAddr {
		node.Free()
		node, err = fs.ReadNode(path.Parent())
		if err != nil {
			node.Free()
			return nil, nil, err
		}
	}
	return path, node, nil
}

func (tree *RawTree) TreeSearch(ctx context.Context, searcher TreeSearcher) (Item, error) {
	path, node, err := tree.search(ctx, searcher.Search)
	if err != nil {
		return Item{}, fmt.Errorf("item with %s: %w", searcher, err)
	}
	item := node.BodyLeaf[path.Node(-1).FromItemSlot]
	item.Body = item.Body.CloneItem()
	node.Free()
	return item, nil
}

func (tree *RawTree) TreeLookup(ctx context.Context, key btrfsprim.Key) (Item, error) {
	return tree.TreeSearch(ctx, SearchExactKey(key))
}

func (tree *RawTree) TreeSearchAll(ctx context.Context, searcher TreeSearcher) ([]Item, error) {
	middlePath, middleNode, err := tree.search(ctx, searcher.Search)
	if err != nil {
		return nil, fmt.Errorf("items with %s: %w", searcher, err)
	}
	middleItem := middleNode.BodyLeaf[middlePath.Node(-1).FromItemSlot]

	ret := []Item{middleItem}
	var errs derror.MultiError
	prevPath, prevNode := middlePath, middleNode
	for {
		prevPath, prevNode, err = tree.Forrest.prev(prevPath, prevNode)
		if err != nil {
			errs = append(errs, err)
			break
		}
		if len(prevPath) == 0 {
			break
		}
		prevItem := prevNode.BodyLeaf[prevPath.Node(-1).FromItemSlot]
		if searcher.Search(prevItem.Key, prevItem.BodySize) != 0 {
			break
		}
		item := prevItem
		item.Body = item.Body.CloneItem()
		ret = append(ret, item)
	}
	slices.Reverse(ret)
	if prevNode.Head.Addr != middlePath.Node(-1).ToNodeAddr {
		prevNode.Free()
		middleNode, err = tree.Forrest.ReadNode(middlePath)
		if err != nil {
			middleNode.Free()
			return nil, fmt.Errorf("items with %s: %w", searcher, err)
		}
	}
	nextPath, nextNode := middlePath, middleNode
	for {
		nextPath, nextNode, err = tree.Forrest.next(nextPath, nextNode)
		if err != nil {
			errs = append(errs, err)
			break
		}
		if len(nextPath) == 0 {
			break
		}
		nextItem := nextNode.BodyLeaf[nextPath.Node(-1).FromItemSlot]
		if searcher.Search(nextItem.Key, nextItem.BodySize) != 0 {
			break
		}
		item := nextItem
		item.Body = item.Body.CloneItem()
		ret = append(ret, item)
	}
	nextNode.Free()
	if errs != nil {
		err = errs
	}
	return ret, err
}
