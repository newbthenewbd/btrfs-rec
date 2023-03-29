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

// TreeOperator is an interface for performing basic btree operations.
type TreeOperator interface {
	// TreeWalk walks a tree, triggering callbacks for every node,
	// key-pointer, and item; as well as for any errors encountered.
	//
	// If the tree is valid, then everything is walked in key-order; but
	// if the tree is broken, then ordering is not guaranteed.
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
	TreeWalk(ctx context.Context, treeID btrfsprim.ObjID, errHandle func(*TreeError), cbs TreeWalkHandler)

	TreeLookup(treeID btrfsprim.ObjID, key btrfsprim.Key) (Item, error)
	TreeSearch(treeID btrfsprim.ObjID, search TreeSearcher) (Item, error)

	// If some items are able to be read, but there is an error reading the
	// full set, then it might return *both* a list of items and an error.
	//
	// If the tree is not found, an error that is ErrNoTree is
	// returned.
	//
	// If no such item is found, an error that is ErrNoItem is
	// returned.
	TreeSearchAll(treeID btrfsprim.ObjID, search TreeSearcher) ([]Item, error)
}

type TreeError struct {
	Path Path
	Err  error
}

func (e *TreeError) Unwrap() error { return e.Err }

func (e *TreeError) Error() string {
	return fmt.Sprintf("%v: %v", e.Path, e.Err)
}

type NodeSource interface {
	Superblock() (*Superblock, error)
	AcquireNode(ctx context.Context, addr btrfsvol.LogicalAddr, exp NodeExpectations) (*Node, error)
	ReleaseNode(*Node)
}
