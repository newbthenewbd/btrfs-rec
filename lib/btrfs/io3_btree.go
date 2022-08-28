// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"context"
	"fmt"
	"io"
	iofs "io/fs"
	"math"
	"strings"

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

var _ Trees = (*FS)(nil)

// - The first element will always have an ItemIdx of -1.
//
//   - For .Item() callbacks, the last element will always have a
//     NodeAddr of 0.
//
// For example, given the tree structure
//
//	[superblock]
//	     |
//	     | <------------------------------------------ pathElem={idx:-1, addr:0x01, lvl:3}
//	     |
//	  +[0x01]-----------+
//	  | lvl=3           |
//	  +-+-+-+-+-+-+-+-+-+
//	  |1|2|3|4|5|6|7|8|9|
//	  +---+---+---+---+-+
//	                 |
//	                 | <------------------------------ pathElem={idx:8, addr:0x02, lvl:2}
//	                 |
//	              +[0x02]-----------+
//	              | lvl=2           |
//	              +-+-+-+-+-+-+-+-+-+
//	              |1|2|3|4|5|6|7|8|9|
//	              +---+---+---+---+-+
//	                           |
//	                           | <-------------------- pathElem={idx:7, addr:0x03, lvl:1}
//	                           |
//	                        +[0x03]-----------+
//	                        | lvl=1           |
//	                        +-+-+-+-+-+-+-+-+-+
//	                        |1|2|3|4|5|6|7|8|9|
//	                        +---+---+---+---+-+
//	                               |
//	                               | <---------------- pathElem={idx:4, addr:0x04, lvl:0}
//	                               |
//	                             +[0x04]-----------+
//	                             | lvl=0           |
//	                             +-+-+-+-+-+-+-+-+-+
//	                             |1|2|3|4|5|6|7|8|9|
//	                             +---+---+---+---+-+
//	                                |
//	                                | <--------------- pathElem={idx:5, addr:0, lvl:0}
//	                                |
//	                              [item]
//
// the path would be
//
//	{-1, 0x01, 3}→{8, 0x02, 2}→{7, 0x03, 1}→{4, 0x04, 0}→{2, 0, 0}
type TreePath struct {
	TreeID ObjID
	Nodes  []TreePathElem
}

// A TreePathElem essentially represents a KeyPointer.
type TreePathElem struct {
	// ItemIdx is the index of this KeyPointer in the parent Node;
	// or -1 if this is the root and there is no KeyPointer.
	ItemIdx int
	// NodeAddr is the address of the node that the KeyPointer
	// points at, or 0 if this is a leaf item and nothing is
	// being pointed at.
	NodeAddr btrfsvol.LogicalAddr
	// NodeLevel is the expected or actual level of the node at
	// NodeAddr.
	NodeLevel uint8
}

func (elem TreePathElem) writeNodeTo(w io.Writer) {
	fmt.Fprintf(w, "node:%d@%v", elem.NodeLevel, elem.NodeAddr)
}

func (path TreePath) String() string {
	var ret strings.Builder
	fmt.Fprintf(&ret, "%s->", path.TreeID.Format(btrfsitem.ROOT_ITEM_KEY))
	if len(path.Nodes) == 0 {
		ret.WriteString("(empty-path)")
	} else {
		path.Nodes[0].writeNodeTo(&ret)
		for _, elem := range path.Nodes[1:] {
			fmt.Fprintf(&ret, "[%v]", elem.ItemIdx)
			if elem.NodeAddr != 0 {
				ret.WriteString("->")
				elem.writeNodeTo(&ret)
			}
		}
	}
	return ret.String()
}

func (path TreePath) DeepCopy() TreePath {
	return TreePath{
		TreeID: path.TreeID,
		Nodes:  append([]TreePathElem(nil), path.Nodes...),
	}
}

func (path TreePath) Append(elem TreePathElem) TreePath {
	path.Nodes = append(path.Nodes, elem)
	return path
}

// path.Node(x) is like path.Nodes[x], but negative values of x move
// down from the end of path.Nodes (similar to how lists work in many
// other languages, such as Python).
func (path TreePath) Node(x int) *TreePathElem {
	if x < 0 {
		x += len(path.Nodes)
	}
	return &path.Nodes[x]
}

type TreeError struct {
	Path TreePath
	Err  error
}

func (e *TreeError) Unwrap() error { return e.Err }

func (e *TreeError) Error() string {
	return fmt.Sprintf("%v: %v", e.Path, e.Err)
}

// A TreeRoot is more-or-less a btrfsitem.Root, but simpler; returned by
// LookupTreeRoot.
type TreeRoot struct {
	TreeID     ObjID
	RootNode   btrfsvol.LogicalAddr
	Level      uint8
	Generation Generation
}

// LookupTreeRoot is a utility function to help with implementing the 'Trees'
// interface.
func LookupTreeRoot(fs Trees, treeID ObjID) (*TreeRoot, error) {
	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}
	switch treeID {
	case ROOT_TREE_OBJECTID:
		return &TreeRoot{
			TreeID:     treeID,
			RootNode:   sb.RootTree,
			Level:      sb.RootLevel,
			Generation: sb.Generation, // XXX: same generation as LOG_TREE?
		}, nil
	case CHUNK_TREE_OBJECTID:
		return &TreeRoot{
			TreeID:     treeID,
			RootNode:   sb.ChunkTree,
			Level:      sb.ChunkLevel,
			Generation: sb.ChunkRootGeneration,
		}, nil
	case TREE_LOG_OBJECTID:
		return &TreeRoot{
			TreeID:     treeID,
			RootNode:   sb.LogTree,
			Level:      sb.LogLevel,
			Generation: sb.Generation, // XXX: same generation as ROOT_TREE?
		}, nil
	case BLOCK_GROUP_TREE_OBJECTID:
		return &TreeRoot{
			TreeID:     treeID,
			RootNode:   sb.BlockGroupRoot,
			Level:      sb.BlockGroupRootLevel,
			Generation: sb.BlockGroupRootGeneration,
		}, nil
	default:
		rootItem, err := fs.TreeSearch(ROOT_TREE_OBJECTID, func(key Key, _ uint32) int {
			if key.ObjectID == treeID && key.ItemType == btrfsitem.ROOT_ITEM_KEY {
				return 0
			}
			return Key{
				ObjectID: treeID,
				ItemType: btrfsitem.ROOT_ITEM_KEY,
				Offset:   0,
			}.Cmp(key)
		})
		if err != nil {
			return nil, err
		}
		rootItemBody, ok := rootItem.Body.(btrfsitem.Root)
		if !ok {
			return nil, fmt.Errorf("malformed ROOT_ITEM for tree %v", treeID)
		}
		return &TreeRoot{
			TreeID:     treeID,
			RootNode:   rootItemBody.ByteNr,
			Level:      rootItemBody.Level,
			Generation: rootItemBody.Generation,
		}, nil
	}
}

type TreeWalkHandler struct {
	// Callbacks for entire nodes
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

func (fs *FS) TreeWalk(ctx context.Context, treeID ObjID, errHandle func(*TreeError), cbs TreeWalkHandler) {
	path := TreePath{
		TreeID: treeID,
	}
	rootInfo, err := LookupTreeRoot(fs, treeID)
	if err != nil {
		errHandle(&TreeError{Path: path, Err: err})
		return
	}
	path = path.Append(TreePathElem{
		ItemIdx:   -1,
		NodeAddr:  rootInfo.RootNode,
		NodeLevel: rootInfo.Level,
	})
	fs.treeWalk(ctx, path, errHandle, cbs)
}

// TreeWalk is a utility function to help with implementing the 'Trees'
// interface.
func (fs *FS) RawTreeWalk(ctx context.Context, rootInfo TreeRoot, errHandle func(*TreeError), cbs TreeWalkHandler) {
	path := TreePath{
		TreeID: rootInfo.TreeID,
		Nodes: []TreePathElem{
			{
				ItemIdx:   -1,
				NodeAddr:  rootInfo.RootNode,
				NodeLevel: rootInfo.Level,
			},
		},
	}
	fs.treeWalk(ctx, path, errHandle, cbs)
}

func (fs *FS) treeWalk(ctx context.Context, path TreePath, errHandle func(*TreeError), cbs TreeWalkHandler) {
	if ctx.Err() != nil {
		return
	}
	if path.Node(-1).NodeAddr == 0 {
		return
	}

	if cbs.PreNode != nil {
		if err := cbs.PreNode(path); err != nil {
			errHandle(&TreeError{Path: path, Err: err})
		}
		if ctx.Err() != nil {
			return
		}
	}
	node, err := fs.readNodeAtLevel(path.Node(-1).NodeAddr, path.Node(-1).NodeLevel)
	if ctx.Err() != nil {
		return
	}
	if err != nil && node != nil && cbs.BadNode != nil {
		// opportunity to fix the node
		err = cbs.BadNode(path, node, err)
	}
	if err != nil {
		errHandle(&TreeError{Path: path, Err: err})
	} else {
		if cbs.Node != nil {
			if err := cbs.Node(path, node); err != nil {
				errHandle(&TreeError{Path: path, Err: err})
			}
		}
	}
	if ctx.Err() != nil {
		return
	}
	if node != nil {
		for i, item := range node.Data.BodyInternal {
			itemPath := path.Append(TreePathElem{
				ItemIdx:   i,
				NodeAddr:  item.BlockPtr,
				NodeLevel: node.Data.Head.Level - 1,
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
			itemPath := path.Append(TreePathElem{
				ItemIdx: i,
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
			errHandle(&TreeError{Path: path, Err: err})
		}
	}
}

func (fs *FS) treeSearch(treeRoot TreeRoot, fn func(Key, uint32) int) (TreePath, *diskio.Ref[btrfsvol.LogicalAddr, Node], error) {
	path := TreePath{
		TreeID: treeRoot.TreeID,
		Nodes: []TreePathElem{
			{
				ItemIdx:   -1,
				NodeAddr:  treeRoot.RootNode,
				NodeLevel: treeRoot.Level,
			},
		},
	}
	for {
		if path.Node(-1).NodeAddr == 0 {
			return TreePath{}, nil, iofs.ErrNotExist
		}
		node, err := fs.readNodeAtLevel(path.Node(-1).NodeAddr, path.Node(-1).NodeLevel)
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
			path = path.Append(TreePathElem{
				ItemIdx:   lastGood,
				NodeAddr:  node.Data.BodyInternal[lastGood].BlockPtr,
				NodeLevel: node.Data.Head.Level - 1,
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
			path = path.Append(TreePathElem{
				ItemIdx: idx,
			})
			return path, node, nil
		}
	}
}

func (fs *FS) prev(path TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, Node]) (TreePath, *diskio.Ref[btrfsvol.LogicalAddr, Node], error) {
	var err error
	path = path.DeepCopy()

	// go up
	for path.Node(-1).ItemIdx < 1 {
		path.Nodes = path.Nodes[:len(path.Nodes)-1]
		if len(path.Nodes) == 0 {
			return TreePath{}, nil, nil
		}
	}
	// go left
	path.Node(-1).ItemIdx--
	if path.Node(-1).NodeAddr != 0 {
		if node.Addr != path.Node(-2).NodeAddr {
			node, err = fs.readNodeAtLevel(path.Node(-2).NodeAddr, path.Node(-2).NodeLevel)
			if err != nil {
				return TreePath{}, nil, err
			}
			path.Node(-1).NodeAddr = node.Data.BodyInternal[path.Node(-1).ItemIdx].BlockPtr
		}
	}
	// go down
	for path.Node(-1).NodeAddr != 0 {
		if node.Addr != path.Node(-1).NodeAddr {
			node, err = fs.readNodeAtLevel(path.Node(-1).NodeAddr, path.Node(-1).NodeLevel)
			if err != nil {
				return TreePath{}, nil, err
			}
		}
		if node.Data.Head.Level > 0 {
			path = path.Append(TreePathElem{
				ItemIdx:   len(node.Data.BodyInternal) - 1,
				NodeAddr:  node.Data.BodyInternal[len(node.Data.BodyInternal)-1].BlockPtr,
				NodeLevel: node.Data.Head.Level - 1,
			})
		} else {
			path = path.Append(TreePathElem{
				ItemIdx: len(node.Data.BodyLeaf) - 1,
			})
		}
	}
	// return
	if node.Addr != path.Node(-2).NodeAddr {
		node, err = fs.readNodeAtLevel(path.Node(-2).NodeAddr, path.Node(-2).NodeLevel)
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
	if node.Addr != path.Node(-2).NodeAddr {
		node, err = fs.readNodeAtLevel(path.Node(-2).NodeAddr, path.Node(-2).NodeLevel)
		if err != nil {
			return TreePath{}, nil, err
		}
		path.Node(-2).NodeLevel = node.Data.Head.Level
	}
	for path.Node(-1).ItemIdx+1 >= int(node.Data.Head.NumItems) {
		path.Nodes = path.Nodes[:len(path.Nodes)-1]
		if len(path.Nodes) == 1 {
			return TreePath{}, nil, nil
		}
		if node.Addr != path.Node(-2).NodeAddr {
			node, err = fs.readNodeAtLevel(path.Node(-2).NodeAddr, path.Node(-2).NodeLevel)
			if err != nil {
				return TreePath{}, nil, err
			}
			path.Node(-2).NodeLevel = node.Data.Head.Level
		}
	}
	// go left
	path.Node(-1).ItemIdx++
	if path.Node(-1).NodeAddr != 0 {
		if node.Addr != path.Node(-2).NodeAddr {
			node, err = fs.readNodeAtLevel(path.Node(-2).NodeAddr, path.Node(-2).NodeLevel)
			if err != nil {
				return TreePath{}, nil, err
			}
			path.Node(-1).NodeAddr = node.Data.BodyInternal[path.Node(-1).ItemIdx].BlockPtr
		}
	}
	// go down
	for path.Node(-1).NodeAddr != 0 {
		if node.Addr != path.Node(-1).NodeAddr {
			node, err = fs.readNodeAtLevel(path.Node(-1).NodeAddr, path.Node(-1).NodeLevel)
			if err != nil {
				return TreePath{}, nil, err
			}
			path.Node(-1).NodeLevel = node.Data.Head.Level
		}
		if node.Data.Head.Level > 0 {
			path = path.Append(TreePathElem{
				ItemIdx:   0,
				NodeAddr:  node.Data.BodyInternal[len(node.Data.BodyInternal)-1].BlockPtr,
				NodeLevel: node.Data.Head.Level - 1,
			})
		} else {
			path = path.Append(TreePathElem{
				ItemIdx: 0,
			})
		}
	}
	// return
	if node.Addr != path.Node(-2).NodeAddr {
		node, err = fs.readNodeAtLevel(path.Node(-2).NodeAddr, path.Node(-2).NodeLevel)
		if err != nil {
			return TreePath{}, nil, err
		}
	}
	return path, node, nil
}

func (fs *FS) TreeSearch(treeID ObjID, fn func(Key, uint32) int) (Item, error) {
	rootInfo, err := LookupTreeRoot(fs, treeID)
	if err != nil {
		return Item{}, err
	}
	path, node, err := fs.treeSearch(*rootInfo, fn)
	if err != nil {
		return Item{}, err
	}
	return node.Data.BodyLeaf[path.Node(-1).ItemIdx], nil
}

func KeySearch(fn func(Key) int) func(Key, uint32) int {
	return func(key Key, _ uint32) int {
		return fn(key)
	}
}

func (fs *FS) TreeLookup(treeID ObjID, key Key) (Item, error) {
	item, err := fs.TreeSearch(treeID, KeySearch(key.Cmp))
	if err != nil {
		err = fmt.Errorf("item with key=%v: %w", key, err)
	}
	return item, err
}

func (fs *FS) TreeSearchAll(treeID ObjID, fn func(Key, uint32) int) ([]Item, error) {
	rootInfo, err := LookupTreeRoot(fs, treeID)
	if err != nil {
		return nil, err
	}
	middlePath, middleNode, err := fs.treeSearch(*rootInfo, fn)
	if err != nil {
		return nil, err
	}
	middleItem := middleNode.Data.BodyLeaf[middlePath.Node(-1).ItemIdx]

	var ret = []Item{middleItem}
	var errs derror.MultiError
	for prevPath, prevNode := middlePath, middleNode; true; {
		prevPath, prevNode, err = fs.prev(prevPath, prevNode)
		if err != nil {
			errs = append(errs, err)
			break
		}
		if len(prevPath.Nodes) == 0 {
			break
		}
		prevItem := prevNode.Data.BodyLeaf[prevPath.Node(-1).ItemIdx]
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
		if len(nextPath.Nodes) == 0 {
			break
		}
		nextItem := nextNode.Data.BodyLeaf[nextPath.Node(-1).ItemIdx]
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
