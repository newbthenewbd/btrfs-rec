// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
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

var maxKey = btrfsprim.Key{
	ObjectID: math.MaxUint64,
	ItemType: math.MaxUint8,
	Offset:   math.MaxUint64,
}

func keyMm(key btrfsprim.Key) btrfsprim.Key {
	switch {
	case key.Offset > 0:
		key.Offset--
	case key.ItemType > 0:
		key.ItemType--
	case key.ObjectID > 0:
		key.ObjectID--
	}
	return key
}

// TreeOperator is an interface for performing basic btree operations.
type TreeOperator interface {
	// TreeWalk walks a tree, triggering callbacks for every node,
	// key-pointer, and item; as well as for any errors encountered.
	//
	// If the tree is valid, then everything is walked in key-order; but if
	// the tree is broken, then ordering is not guaranteed.
	//
	// Canceling the Context causes TreeWalk to return early; no
	// values from the Context are used.
	//
	// The lifecycle of callbacks is:
	//
	//     001 .PreNode()
	//     002 (read node)
	//     003 .Node() (or .BadNode())
	//         for item in node.items:
	//           if btrfsprim:
	//     004     .PreKeyPointer()
	//     005     (recurse)
	//     006     .PostKeyPointer()
	//           else:
	//     004     .Item() (or .BadItem())
	//     007 .PostNode()
	TreeWalk(ctx context.Context, treeID btrfsprim.ObjID, errHandle func(*TreeError), cbs TreeWalkHandler)

	TreeLookup(treeID btrfsprim.ObjID, key btrfsprim.Key) (Item, error)
	TreeSearch(treeID btrfsprim.ObjID, fn func(key btrfsprim.Key, size uint32) int) (Item, error) // size is math.MaxUint32 for key-pointers

	// If some items are able to be read, but there is an error reading the
	// full set, then it might return *both* a list of items and an error.
	//
	// If no such item is found, an error that is io/fs.ErrNotExist is
	// returned.
	TreeSearchAll(treeID btrfsprim.ObjID, fn func(key btrfsprim.Key, size uint32) int) ([]Item, error) // size is math.MaxUint32 for key-pointers
}

type TreeWalkHandler struct {
	// Callbacks for entire nodes.
	//
	// If any of these return an error that is io/fs.SkipDir, the
	// node immediately stops getting processed; if PreNode, Node,
	// or BadNode return io/fs.SkipDir then key pointers and items
	// within the node are not processed.
	PreNode  func(TreePath) error
	Node     func(TreePath, *diskio.Ref[btrfsvol.LogicalAddr, Node]) error
	BadNode  func(TreePath, *diskio.Ref[btrfsvol.LogicalAddr, Node], error) error
	PostNode func(TreePath, *diskio.Ref[btrfsvol.LogicalAddr, Node]) error
	// Callbacks for items on btrfsprim nodes
	PreKeyPointer  func(TreePath, KeyPointer) error
	PostKeyPointer func(TreePath, KeyPointer) error
	// Callbacks for items on leaf nodes
	Item    func(TreePath, Item) error
	BadItem func(TreePath, Item) error
}

type TreeError struct {
	Path TreePath
	Err  error
}

func (e *TreeError) Unwrap() error { return e.Err }

func (e *TreeError) Error() string {
	return fmt.Sprintf("%v: %v", e.Path, e.Err)
}

type NodeSource interface {
	Superblock() (*Superblock, error)
	ReadNode(TreePath) (*diskio.Ref[btrfsvol.LogicalAddr, Node], error)
}

type TreeOperatorImpl struct {
	NodeSource
}

// TreeWalk implements the 'Trees' interface.
func (fs TreeOperatorImpl) TreeWalk(ctx context.Context, treeID btrfsprim.ObjID, errHandle func(*TreeError), cbs TreeWalkHandler) {
	sb, err := fs.Superblock()
	if err != nil {
		errHandle(&TreeError{Path: TreePath{{FromTree: treeID, ToMaxKey: maxKey}}, Err: err})
	}
	rootInfo, err := LookupTreeRoot(fs, *sb, treeID)
	if err != nil {
		errHandle(&TreeError{Path: TreePath{{FromTree: treeID, ToMaxKey: maxKey}}, Err: err})
		return
	}
	fs.RawTreeWalk(ctx, *rootInfo, errHandle, cbs)
}

// TreeWalk is a utility method to help with implementing the 'Trees'.
// interface.
func (fs TreeOperatorImpl) RawTreeWalk(ctx context.Context, rootInfo TreeRoot, errHandle func(*TreeError), cbs TreeWalkHandler) {
	path := TreePath{{
		FromTree:         rootInfo.TreeID,
		FromItemIdx:      -1,
		ToNodeAddr:       rootInfo.RootNode,
		ToNodeGeneration: rootInfo.Generation,
		ToNodeLevel:      rootInfo.Level,
		ToMaxKey:         maxKey,
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
	} else {
		if cbs.Node != nil {
			if err := cbs.Node(path, node); err != nil {
				if errors.Is(err, iofs.SkipDir) {
					return
				}
				errHandle(&TreeError{Path: path, Err: err})
			}
		}
	}
	if ctx.Err() != nil {
		return
	}
	if node != nil {
		for i, item := range node.Data.BodyInternal {
			toMaxKey := path.Node(-1).ToMaxKey
			if i+1 < len(node.Data.BodyInternal) {
				toMaxKey = keyMm(node.Data.BodyInternal[i+1].Key)
			}
			itemPath := append(path, TreePathElem{
				FromTree:         node.Data.Head.Owner,
				FromItemIdx:      i,
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
				FromTree:    node.Data.Head.Owner,
				FromItemIdx: i,
				ToKey:       item.Key,
				ToMaxKey:    item.Key,
			})
			if errBody, isErr := item.Body.(btrfsitem.Error); isErr {
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
		FromItemIdx:      -1,
		ToNodeAddr:       treeRoot.RootNode,
		ToNodeGeneration: treeRoot.Generation,
		ToNodeLevel:      treeRoot.Level,
		ToMaxKey:         maxKey,
	}}
	for {
		if path.Node(-1).ToNodeAddr == 0 {
			return nil, nil, iofs.ErrNotExist
		}
		node, err := fs.ReadNode(path)
		if err != nil {
			return nil, nil, err
		}

		if node.Data.Head.Level > 0 {
			// btrfsprim node

			// Search for the right-most node.Data.BodyInternal item for which
			// `fn(item.Key) >= 0`.
			//
			//    + + + + 0 - - - -
			//
			// There may or may not be a value that returns '0'.
			//
			// i.e. find the highest value that isn't too high.
			lastGood, ok := slices.SearchHighest(node.Data.BodyInternal, func(kp KeyPointer) int {
				return slices.Min(fn(kp.Key, math.MaxUint32), 0) // don't return >0; a key can't be "too low"
			})
			if !ok {
				return nil, nil, iofs.ErrNotExist
			}
			toMaxKey := path.Node(-1).ToMaxKey
			if lastGood+1 < len(node.Data.BodyInternal) {
				toMaxKey = keyMm(node.Data.BodyInternal[lastGood+1].Key)
			}
			path = append(path, TreePathElem{
				FromTree:         node.Data.Head.Owner,
				FromItemIdx:      lastGood,
				ToNodeAddr:       node.Data.BodyInternal[lastGood].BlockPtr,
				ToNodeGeneration: node.Data.BodyInternal[lastGood].Generation,
				ToNodeLevel:      node.Data.Head.Level - 1,
				ToKey:            node.Data.BodyInternal[lastGood].Key,
				ToMaxKey:         toMaxKey,
			})
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
			idx, ok := slices.Search(node.Data.BodyLeaf, func(item Item) int {
				return fn(item.Key, item.BodySize)
			})
			if !ok {
				return nil, nil, iofs.ErrNotExist
			}
			path = append(path, TreePathElem{
				FromTree:    node.Data.Head.Owner,
				FromItemIdx: idx,
				ToKey:       node.Data.BodyLeaf[idx].Key,
				ToMaxKey:    node.Data.BodyLeaf[idx].Key,
			})
			return path, node, nil
		}
	}
}

func (fs TreeOperatorImpl) prev(path TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, Node]) (TreePath, *diskio.Ref[btrfsvol.LogicalAddr, Node], error) {
	var err error
	path = path.DeepCopy()

	// go up
	for path.Node(-1).FromItemIdx < 1 {
		path = path.Parent()
		if len(path) == 0 {
			return nil, nil, nil
		}
	}
	// go left
	path.Node(-1).FromItemIdx--
	if path.Node(-1).ToNodeAddr != 0 {
		if node.Addr != path.Node(-2).ToNodeAddr {
			node, err = fs.ReadNode(path.Parent())
			if err != nil {
				return nil, nil, err
			}
			path.Node(-1).ToNodeAddr = node.Data.BodyInternal[path.Node(-1).FromItemIdx].BlockPtr
		}
	}
	// go down
	for path.Node(-1).ToNodeAddr != 0 {
		if node.Addr != path.Node(-1).ToNodeAddr {
			node, err = fs.ReadNode(path)
			if err != nil {
				return nil, nil, err
			}
		}
		if node.Data.Head.Level > 0 {
			path = append(path, TreePathElem{
				FromTree:         node.Data.Head.Owner,
				FromItemIdx:      len(node.Data.BodyInternal) - 1,
				ToNodeAddr:       node.Data.BodyInternal[len(node.Data.BodyInternal)-1].BlockPtr,
				ToNodeGeneration: node.Data.BodyInternal[len(node.Data.BodyInternal)-1].Generation,
				ToNodeLevel:      node.Data.Head.Level - 1,
				ToKey:            node.Data.BodyInternal[len(node.Data.BodyInternal)-1].Key,
				ToMaxKey:         path.Node(-1).ToMaxKey,
			})
		} else {
			path = append(path, TreePathElem{
				FromTree:    node.Data.Head.Owner,
				FromItemIdx: len(node.Data.BodyLeaf) - 1,
				ToKey:       node.Data.BodyLeaf[len(node.Data.BodyLeaf)-1].Key,
				ToMaxKey:    node.Data.BodyLeaf[len(node.Data.BodyLeaf)-1].Key,
			})
		}
	}
	// return
	if node.Addr != path.Node(-2).ToNodeAddr {
		node, err = fs.ReadNode(path.Parent())
		if err != nil {
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
		node, err = fs.ReadNode(path.Parent())
		if err != nil {
			return nil, nil, err
		}
		path.Node(-2).ToNodeLevel = node.Data.Head.Level
	}
	for path.Node(-1).FromItemIdx+1 >= int(node.Data.Head.NumItems) {
		path = path.Parent()
		if len(path) == 1 {
			return nil, nil, nil
		}
		if node.Addr != path.Node(-2).ToNodeAddr {
			node, err = fs.ReadNode(path.Parent())
			if err != nil {
				return nil, nil, err
			}
			path.Node(-2).ToNodeLevel = node.Data.Head.Level
		}
	}
	// go right
	path.Node(-1).FromItemIdx++
	if path.Node(-1).ToNodeAddr != 0 {
		if node.Addr != path.Node(-2).ToNodeAddr {
			node, err = fs.ReadNode(path.Parent())
			if err != nil {
				return nil, nil, err
			}
			path.Node(-1).ToNodeAddr = node.Data.BodyInternal[path.Node(-1).FromItemIdx].BlockPtr
		}
	}
	// go down
	for path.Node(-1).ToNodeAddr != 0 {
		if node.Addr != path.Node(-1).ToNodeAddr {
			node, err = fs.ReadNode(path)
			if err != nil {
				return nil, nil, err
			}
			path.Node(-1).ToNodeLevel = node.Data.Head.Level
		}
		if node.Data.Head.Level > 0 {
			toMaxKey := path.Node(-1).ToMaxKey
			if len(node.Data.BodyInternal) > 1 {
				toMaxKey = keyMm(node.Data.BodyInternal[1].Key)
			}
			path = append(path, TreePathElem{
				FromTree:         node.Data.Head.Owner,
				FromItemIdx:      0,
				ToNodeAddr:       node.Data.BodyInternal[0].BlockPtr,
				ToNodeGeneration: node.Data.BodyInternal[0].Generation,
				ToNodeLevel:      node.Data.Head.Level - 1,
				ToKey:            node.Data.BodyInternal[0].Key,
				ToMaxKey:         toMaxKey,
			})
		} else {
			path = append(path, TreePathElem{
				FromTree:    node.Data.Head.Owner,
				FromItemIdx: 0,
				ToKey:       node.Data.BodyInternal[0].Key,
				ToMaxKey:    node.Data.BodyInternal[0].Key,
			})
		}
	}
	// return
	if node.Addr != path.Node(-2).ToNodeAddr {
		node, err = fs.ReadNode(path.Parent())
		if err != nil {
			return nil, nil, err
		}
	}
	return path, node, nil
}

// TreeSearch implements the 'Trees' interface.
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
	return node.Data.BodyLeaf[path.Node(-1).FromItemIdx], nil
}

// KeySearch returns a comparator suitable to be passed to TreeSearch.
func KeySearch(fn func(btrfsprim.Key) int) func(btrfsprim.Key, uint32) int {
	return func(key btrfsprim.Key, _ uint32) int {
		return fn(key)
	}
}

// TreeLookup implements the 'Trees' interface.
func (fs TreeOperatorImpl) TreeLookup(treeID btrfsprim.ObjID, key btrfsprim.Key) (Item, error) {
	item, err := fs.TreeSearch(treeID, KeySearch(key.Cmp))
	if err != nil {
		err = fmt.Errorf("item with key=%v: %w", key, err)
	}
	return item, err
}

// TreeSearchAll implements the 'Trees' interface.
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
	middleItem := middleNode.Data.BodyLeaf[middlePath.Node(-1).FromItemIdx]

	var ret = []Item{middleItem}
	var errs derror.MultiError
	for prevPath, prevNode := middlePath, middleNode; true; {
		prevPath, prevNode, err = fs.prev(prevPath, prevNode)
		if err != nil {
			errs = append(errs, err)
			break
		}
		if len(prevPath) == 0 {
			break
		}
		prevItem := prevNode.Data.BodyLeaf[prevPath.Node(-1).FromItemIdx]
		if fn(prevItem.Key, prevItem.BodySize) != 0 {
			break
		}
		ret = append(ret, prevItem)
	}
	slices.Reverse(ret)
	for nextPath, nextNode := middlePath, middleNode; true; {
		nextPath, nextNode, err = fs.next(nextPath, nextNode)
		if err != nil {
			errs = append(errs, err)
			break
		}
		if len(nextPath) == 0 {
			break
		}
		nextItem := nextNode.Data.BodyLeaf[nextPath.Node(-1).FromItemIdx]
		if fn(nextItem.Key, nextItem.BodySize) != 0 {
			break
		}
		ret = append(ret, nextItem)
	}
	if errs != nil {
		err = errs
	}
	return ret, err
}
