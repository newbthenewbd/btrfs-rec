// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree

import (
	"fmt"
	"io"
	"strings"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

// Path is a path from the superblock (i.e. the root of the btrfs
// system) to the a node or item within one of the btrees in the
// system.
//
//   - The first element will always have an ItemSlot of -1.
//
//   - For .Item() callbacks, the last element will always have a
//     NodeAddr of 0.
//
// For example, a path through a tree, with the associated PathElems:
//
//	[superblock: tree=B, lvl=3, gen=6]
//	     |
//	     | <------------------------------------------ pathElem={from_tree:B, from_slot=-1,
//	     |                                                       to_addr:0x01, to_gen=6, to_lvl=3}
//	  +[0x01]-------------+
//	  | lvl=3 gen=6 own=B |
//	  +-+-+-+-+-+-+-+-+-+-+
//	  |0|1|2|3|4|5|6|7|8|9|
//	  +-+-+-+-+-+-+-+-+-+-+
//	                 |
//	                 | <------------------------------ pathElem:{from_tree:B, from_slot:7,
//	                 |                                           to_addr:0x02, to_gen:5, to_lvl:2}
//	              +[0x02]--------------+
//	              | lvl=2 gen=5 own=B  |
//	              +-+-+-+-+-+-+-+-+-+-+
//	              |0|1|2|3|4|5|6|7|8|9|
//	              +-+-+-+-+-+-+-+-+-+-+
//	                           |
//	                           | <-------------------- pathElem={from_tree:B, from_slot:6,
//	                           |                                 to_addr:0x03, to_gen:5, to_lvl:1}
//	                        +[0x03]-------------+
//	                        | lvl=1 gen=5 own=A |
//	                        +-+-+-+-+-+-+-+-+-+-+
//	                        |0|1|2|3|4|5|6|7|8|9|
//	                        +-+-+-+-+-+-+-+-+-+-+
//	                               |
//	                               | <---------------- pathElem={from_tree:A, from_slot:3,
//	                               |                             to_addr:0x04, to_gen:2, lvl:0}
//	                             +[0x04]-------------+
//	                             | lvl=0 gen=2 own=A |
//	                             +-+-+-+-+-+-+-+-+-+-+
//	                             |0|1|2|3|4|5|6|7|8|9|
//	                             +-+-+-+-+-+-+-+-+-+-+
//	                                |
//	                                | <--------------- pathElem={from_tree:A, from_slot:1,
//	                                |                            to_addr:0, to_gen: 0, to_lvl:0}
//	                              [item]
type Path []PathElem

// A PathElem essentially represents a KeyPointer.
type PathElem struct {
	// FromTree is the owning tree ID of the parent node; or the
	// well-known tree ID if this is the root.
	FromTree btrfsprim.ObjID
	// FromItemSlot is the index of this KeyPointer in the parent
	// Node; or -1 if this is the root and there is no KeyPointer.
	FromItemSlot int

	// ToNodeAddr is the address of the node that the KeyPointer
	// points at, or 0 if this is a leaf item and nothing is being
	// pointed at.
	ToNodeAddr btrfsvol.LogicalAddr
	// ToNodeGeneration is the expected generation of the node at
	// ToNodeAddr, or 0 if this is a leaf item and nothing is
	// being pointed at.
	ToNodeGeneration btrfsprim.Generation
	// ToNodeLevel is the expected level of the node at
	// ToNodeAddr, or 0 if this is a leaf item and nothing is
	// being pointed at.
	ToNodeLevel uint8
	// ToKey is either
	//  - btrfprim.Key{} if this is the root node being pointed
	//    to,
	//  - the KeyPointer.Key if this is a non-root node being
	//    pointed to, or
	//  - the key of the leaf item being pointed to.
	ToKey    btrfsprim.Key
	ToMaxKey btrfsprim.Key
}

func (elem PathElem) writeNodeTo(w io.Writer) {
	fmt.Fprintf(w, "node:%d@%v", elem.ToNodeLevel, elem.ToNodeAddr)
}

func (path Path) String() string {
	if len(path) == 0 {
		return "(empty-path)"
	}
	var ret strings.Builder
	fmt.Fprintf(&ret, "%s->", path[0].FromTree.Format(btrfsprim.ROOT_TREE_OBJECTID))
	if len(path) == 1 && path[0] == (PathElem{FromTree: path[0].FromTree, FromItemSlot: -1}) {
		ret.WriteString("(empty-path)")
	} else {
		path[0].writeNodeTo(&ret)
	}
	for _, elem := range path[1:] {
		fmt.Fprintf(&ret, "[%v]", elem.FromItemSlot)
		if elem.ToNodeAddr != 0 {
			ret.WriteString("->")
			elem.writeNodeTo(&ret)
		}
	}
	return ret.String()
}

func (path Path) DeepCopy() Path {
	return append(Path(nil), path...)
}

// NodeExpectations returns the address to read and the expectations
// to have when reading the node pointed to by this Path.
//
// `ok` is false if the path is empty or if this Path points to an
// item rather than a node.
func (path Path) NodeExpectations(fs NodeFile) (_ btrfsvol.LogicalAddr, _ NodeExpectations, ok bool) {
	if path.Node(-1).ToNodeAddr == 0 && path.Node(-1).ToNodeGeneration == 0 && path.Node(-1).ToNodeLevel == 0 {
		return 0, NodeExpectations{}, false
	}

	checkOwner := func(owner btrfsprim.ObjID, gen btrfsprim.Generation) error {
		var treeParents []btrfsprim.ObjID

		tree := path.Node(-1).FromTree
		for {
			if owner == tree {
				// OK!
				return nil
			}

			treeParents = append(treeParents, tree)
			parent, parentGen, parentOK := fs.ParentTree(tree)
			if !parentOK {
				// Failed look up parent info; fail open.
				return nil
			}

			if parent == 0 {
				// End of the line.
				return fmt.Errorf("expected owner in %v but claims to have owner=%v",
					treeParents, owner)
			}
			if gen > parentGen {
				return fmt.Errorf("claimed owner=%v might be acceptable in this tree (if generation<=%v) but not with claimed generation=%v",
					owner, parentGen, gen)
			}
			tree = parent
		}
	}

	return path.Node(-1).ToNodeAddr, NodeExpectations{
		LAddr:      containers.OptionalValue(path.Node(-1).ToNodeAddr),
		Level:      containers.OptionalValue(path.Node(-1).ToNodeLevel),
		Generation: containers.OptionalValue(path.Node(-1).ToNodeGeneration),
		Owner:      checkOwner,
		MinItem:    containers.OptionalValue(path.Node(-1).ToKey),
		MaxItem:    containers.OptionalValue(path.Node(-1).ToMaxKey),
	}, true
}

func (path Path) Parent() Path {
	return path[:len(path)-1]
}

// Node is returns an element from the path; `path.Node(x)` is like
// `&path[x]`, but negative values of x move down from the end of path
// (similar to how lists work in many other languages, such as
// Python).
func (path Path) Node(x int) *PathElem {
	if x < 0 {
		x += len(path)
	}
	return &path[x]
}
