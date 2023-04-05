// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package btrfstree contains core b+-tree implementation and
// interfaces.
package btrfstree

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

type Forrest interface {
	// ForrestLookup returns (nil, ErrNoTree) if the tree does not
	// exist but there is otherwise no error.
	ForrestLookup(ctx context.Context, treeID btrfsprim.ObjID) (Tree, error)
}

type Tree interface {
	// TreeParentID returns the ID of this tree's parent and the
	// generation that this tree was split from its parent.  If
	// this tree has no parent, then (0, 0, nil) is returned.
	TreeParentID(ctx context.Context) (btrfsprim.ObjID, btrfsprim.Generation, error)

	// TreeLookup looks up the Item for a given key.
	//
	// If no such Item exists, but there is otherwise no error,
	// then ErrNoItem is returned.
	TreeLookup(ctx context.Context, key btrfsprim.Key) (Item, error)

	// TreeSearch searches the Tree for a value for which
	// `search.Search(itemKey, itemSize) == 0`.
	//
	//	: + + + 0 0 0 - - -
	//	:       ^ ^ ^
	//	:       any of
	//
	// You can conceptualize `search.Search` as subtraction:
	//
	//	func(strawKey btrfsprim.Key, strawSize uint32) int {
	//	    return needle - straw
	//	}
	//
	// `search.Search` may be called with key/size pairs that do not
	// correspond to an existing Item (for key-pointers in
	// interior nodes in the tree); in which case the size
	// math.MaxUint32.
	//
	// If no such Item exists, but there is otherwise no error,
	// then an error that is ErrNoItem is returned.
	TreeSearch(ctx context.Context, search TreeSearcher) (Item, error)

	// TreeRange iterates over the Tree in order, calling
	// `handleFn` for each Item.
	TreeRange(ctx context.Context, handleFn func(Item) bool) error

	// TreeSubrange iterates over the Tree in order, calling
	// `handleFn` for all Items for which `search.Search` returns
	// 0.
	//
	// `search.Search` may be called with key/size pairs that do
	// not correspond to an existing Item (for key-pointers in
	// interior nodes in the tree); in which case the size
	// math.MaxUint32.
	//
	// If handleFn is called for fewer than `min` items, an error
	// that is ErrNoItem is returned.
	TreeSubrange(ctx context.Context,
		min int,
		search TreeSearcher,
		handleFn func(Item) bool,
	) error

	// TreeWalk is a lower-level call than TreeSubrange.  Use with
	// hesitancy.
	//
	// It walks a Tree, triggering callbacks for every node, key-pointer,
	// and item; as well as for any errors encountered.
	//
	// If the Tree is valid, then everything is walked in key-order; but
	// if the Tree is broken, then ordering is not guaranteed.
	//
	// Canceling the Context causes TreeWalk to return early; no values
	// from the Context are used.
	//
	// The lifecycle of callbacks is:
	//
	//	000  (read superblock) (maybe cbs.BadSuperblock())
	//
	//	001  (read node)
	//	002  cbs.Node() or cbs.BadNode()
	//	     if interior:
	//	       for kp in node.items:
	//	003a     if cbs.PreKeyPointer == nil || cbs.PreKeyPointer() {
	//	004b       (recurse)
	//	     else:
	//	       for item in node.items:
	//	003b     cbs.Item() or cbs.BadItem()
	TreeWalk(ctx context.Context, cbs TreeWalkHandler)
}

type TreeSearcher interface {
	// How the search should be described in the event of an
	// error.
	fmt.Stringer

	// size is math.MaxUint32 for key-pointers
	Search(key btrfsprim.Key, size uint32) int
}

type TreeWalkHandler struct {
	BadSuperblock func(error)

	// Callbacks for entire nodes.
	//
	// The return value from BadNode is whether to process the
	// slots in this node or not; if no BadNode function is given,
	// then it is not processed.
	Node    func(Path, *Node)
	BadNode func(Path, *Node, error) bool

	// Callbacks for slots in nodes.
	//
	// The return value from KeyPointer is whether to recurse or
	// not; if no KeyPointer function is given, then it is
	// recursed.
	KeyPointer func(Path, KeyPointer) bool
	Item       func(Path, Item)
	BadItem    func(Path, Item)
}

type NodeSource interface {
	Superblock() (*Superblock, error)
	AcquireNode(ctx context.Context, addr btrfsvol.LogicalAddr, exp NodeExpectations) (*Node, error)
	ReleaseNode(*Node)
}
