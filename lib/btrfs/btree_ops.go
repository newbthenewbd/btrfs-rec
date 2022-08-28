// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"math"

	"github.com/datawire/dlib/derror"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

type Trees interface {
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
	//           if internal:
	//     004     .PreKeyPointer()
	//     005     (recurse)
	//     006     .PostKeyPointer()
	//           else:
	//     004     .Item() (or .BadItem())
	//     007 .PostNode()
	TreeWalk(ctx context.Context, treeID ObjID, errHandle func(*TreeError), cbs TreeWalkHandler)

	TreeLookup(treeID ObjID, key Key) (Item, error)
	TreeSearch(treeID ObjID, fn func(key Key, size uint32) int) (Item, error) // size is math.MaxUint32 for key-pointers

	// If some items are able to be read, but there is an error reading the
	// full set, then it might return *both* a list of items and an error.
	//
	// If no such item is found, an error that is io/fs.ErrNotExist is
	// returned.
	TreeSearchAll(treeID ObjID, fn func(key Key, size uint32) int) ([]Item, error) // size is math.MaxUint32 for key-pointers

	// For bootstrapping purposes.
	Superblock() (*Superblock, error)

	// For reading raw data extants pointed at by tree items.
	ReadAt(p []byte, off btrfsvol.LogicalAddr) (int, error)
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
	// Callbacks for items on internal nodes
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

// TreeWalk implements the 'Trees' interface.
func (fs *FS) TreeWalk(ctx context.Context, treeID ObjID, errHandle func(*TreeError), cbs TreeWalkHandler) {
	rootInfo, err := LookupTreeRoot(fs, treeID)
	if err != nil {
		errHandle(&TreeError{Path: TreePath{{FromTree: treeID}}, Err: err})
		return
	}
	fs.RawTreeWalk(ctx, *rootInfo, errHandle, cbs)
}

// TreeWalk is a utility function to help with implementing the 'Trees'
// interface.
func (fs *FS) RawTreeWalk(ctx context.Context, rootInfo TreeRoot, errHandle func(*TreeError), cbs TreeWalkHandler) {
	path := TreePath{{
		FromTree:       rootInfo.TreeID,
		FromGeneration: rootInfo.Generation,
		FromItemIdx:    -1,
		ToNodeAddr:     rootInfo.RootNode,
		ToNodeLevel:    rootInfo.Level,
	}}
	fs.treeWalk(ctx, path, errHandle, cbs)
}

func (fs *FS) treeWalk(ctx context.Context, path TreePath, errHandle func(*TreeError), cbs TreeWalkHandler) {
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
			itemPath := append(path, TreePathElem{
				FromTree:       node.Data.Head.Owner,
				FromGeneration: node.Data.Head.Generation,
				FromItemIdx:    i,
				ToNodeAddr:     item.BlockPtr,
				ToNodeLevel:    node.Data.Head.Level - 1,
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
				FromTree:       node.Data.Head.Owner,
				FromGeneration: node.Data.Head.Generation,
				FromItemIdx:    i,
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

func (fs *FS) treeSearch(treeRoot TreeRoot, fn func(Key, uint32) int) (TreePath, *diskio.Ref[btrfsvol.LogicalAddr, Node], error) {
	path := TreePath{{
		FromTree:       treeRoot.TreeID,
		FromGeneration: treeRoot.Generation,
		FromItemIdx:    -1,
		ToNodeAddr:     treeRoot.RootNode,
		ToNodeLevel:    treeRoot.Level,
	}}
	for {
		if path.Node(-1).ToNodeAddr == 0 {
			return TreePath{}, nil, iofs.ErrNotExist
		}
		node, err := fs.ReadNode(path)
		if err != nil {
			return TreePath{}, nil, err
		}

		if node.Data.Head.Level > 0 {
			// internal node

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
				return TreePath{}, nil, iofs.ErrNotExist
			}
			path = append(path, TreePathElem{
				FromTree:       node.Data.Head.Owner,
				FromGeneration: node.Data.Head.Generation,
				FromItemIdx:    lastGood,
				ToNodeAddr:     node.Data.BodyInternal[lastGood].BlockPtr,
				ToNodeLevel:    node.Data.Head.Level - 1,
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
				return TreePath{}, nil, iofs.ErrNotExist
			}
			path = append(path, TreePathElem{
				FromTree:       node.Data.Head.Owner,
				FromGeneration: node.Data.Head.Generation,
				FromItemIdx:    idx,
			})
			return path, node, nil
		}
	}
}

func (fs *FS) prev(path TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, Node]) (TreePath, *diskio.Ref[btrfsvol.LogicalAddr, Node], error) {
	var err error
	path = path.DeepCopy()

	// go up
	for path.Node(-1).FromItemIdx < 1 {
		path = path.Parent()
		if len(path) == 0 {
			return TreePath{}, nil, nil
		}
	}
	// go left
	path.Node(-1).FromItemIdx--
	if path.Node(-1).ToNodeAddr != 0 {
		if node.Addr != path.Node(-2).ToNodeAddr {
			node, err = fs.ReadNode(path.Parent())
			if err != nil {
				return TreePath{}, nil, err
			}
			path.Node(-1).ToNodeAddr = node.Data.BodyInternal[path.Node(-1).FromItemIdx].BlockPtr
		}
	}
	// go down
	for path.Node(-1).ToNodeAddr != 0 {
		if node.Addr != path.Node(-1).ToNodeAddr {
			node, err = fs.ReadNode(path)
			if err != nil {
				return TreePath{}, nil, err
			}
		}
		if node.Data.Head.Level > 0 {
			path = append(path, TreePathElem{
				FromTree:       node.Data.Head.Owner,
				FromGeneration: node.Data.Head.Generation,
				FromItemIdx:    len(node.Data.BodyInternal) - 1,
				ToNodeAddr:     node.Data.BodyInternal[len(node.Data.BodyInternal)-1].BlockPtr,
				ToNodeLevel:    node.Data.Head.Level - 1,
			})
		} else {
			path = append(path, TreePathElem{
				FromTree:       node.Data.Head.Owner,
				FromGeneration: node.Data.Head.Generation,
				FromItemIdx:    len(node.Data.BodyLeaf) - 1,
			})
		}
	}
	// return
	if node.Addr != path.Node(-2).ToNodeAddr {
		node, err = fs.ReadNode(path.Parent())
		if err != nil {
			return TreePath{}, nil, err
		}
	}
	return path, node, nil
}

func (fs *FS) next(path TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, Node]) (TreePath, *diskio.Ref[btrfsvol.LogicalAddr, Node], error) {
	var err error
	path = path.DeepCopy()

	// go up
	if node.Addr != path.Node(-2).ToNodeAddr {
		node, err = fs.ReadNode(path.Parent())
		if err != nil {
			return TreePath{}, nil, err
		}
		path.Node(-2).ToNodeLevel = node.Data.Head.Level
	}
	for path.Node(-1).FromItemIdx+1 >= int(node.Data.Head.NumItems) {
		path = path.Parent()
		if len(path) == 1 {
			return TreePath{}, nil, nil
		}
		if node.Addr != path.Node(-2).ToNodeAddr {
			node, err = fs.ReadNode(path.Parent())
			if err != nil {
				return TreePath{}, nil, err
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
				return TreePath{}, nil, err
			}
			path.Node(-1).ToNodeAddr = node.Data.BodyInternal[path.Node(-1).FromItemIdx].BlockPtr
		}
	}
	// go down
	for path.Node(-1).ToNodeAddr != 0 {
		if node.Addr != path.Node(-1).ToNodeAddr {
			node, err = fs.ReadNode(path)
			if err != nil {
				return TreePath{}, nil, err
			}
			path.Node(-1).ToNodeLevel = node.Data.Head.Level
		}
		if node.Data.Head.Level > 0 {
			path = append(path, TreePathElem{
				FromTree:       node.Data.Head.Owner,
				FromGeneration: node.Data.Head.Generation,
				FromItemIdx:    0,
				ToNodeAddr:     node.Data.BodyInternal[len(node.Data.BodyInternal)-1].BlockPtr,
				ToNodeLevel:    node.Data.Head.Level - 1,
			})
		} else {
			path = append(path, TreePathElem{
				FromTree:       node.Data.Head.Owner,
				FromGeneration: node.Data.Head.Generation,
				FromItemIdx:    0,
			})
		}
	}
	// return
	if node.Addr != path.Node(-2).ToNodeAddr {
		node, err = fs.ReadNode(path.Parent())
		if err != nil {
			return TreePath{}, nil, err
		}
	}
	return path, node, nil
}

// TreeSearch implements the 'Trees' interface.
func (fs *FS) TreeSearch(treeID ObjID, fn func(Key, uint32) int) (Item, error) {
	rootInfo, err := LookupTreeRoot(fs, treeID)
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
func KeySearch(fn func(Key) int) func(Key, uint32) int {
	return func(key Key, _ uint32) int {
		return fn(key)
	}
}

// TreeLookup implements the 'Trees' interface.
func (fs *FS) TreeLookup(treeID ObjID, key Key) (Item, error) {
	item, err := fs.TreeSearch(treeID, KeySearch(key.Cmp))
	if err != nil {
		err = fmt.Errorf("item with key=%v: %w", key, err)
	}
	return item, err
}

// TreeSearchAll implements the 'Trees' interface.
func (fs *FS) TreeSearchAll(treeID ObjID, fn func(Key, uint32) int) ([]Item, error) {
	rootInfo, err := LookupTreeRoot(fs, treeID)
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
