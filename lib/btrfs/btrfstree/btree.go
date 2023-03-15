// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

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
	//           if interior:
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
	// If the tree is not found, an error that is ErrNoTree is
	// returned.
	//
	// If no such item is found, an error that is ErrNoItem is
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
	// Callbacks for items on interior nodes
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
