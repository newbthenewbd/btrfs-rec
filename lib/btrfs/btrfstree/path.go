// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree

import (
	"context"
	"fmt"
	"strings"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

// Path is a path from the superblock or a ROOT_ITEM to a node or
// item within one of the btrees in the system.
//
//   - The first element will always have a FromSlot of -1.
//
//   - For .Item() callbacks, the last element will always have a
//     NodeAddr of 0.
//
// For example, a path through a tree, with the associated PathElems:
//
//	[superblock: tree=B, lvl=3, gen=6]
//	     |
//	     | <------------------------------------------ PathRoot={Tree:B,
//	     |                                                       ToAddr:0x01, ToGen=6, ToLvl=3}
//	  +[0x01]-------------+
//	  | lvl=3 gen=6 own=B |
//	  +-+-+-+-+-+-+-+-+-+-+
//	  |0|1|2|3|4|5|6|7|8|9|
//	  +-+-+-+-+-+-+-+-+-+-+
//	                 |
//	                 | <------------------------------ PathKP={FromTree:B, FromSlot:7,
//	                 |                                         ToAddr:0x02, ToGen:5, ToLvl:2}
//	              +[0x02]--------------+
//	              | lvl=2 gen=5 own=B  |
//	              +-+-+-+-+-+-+-+-+-+-+
//	              |0|1|2|3|4|5|6|7|8|9|
//	              +-+-+-+-+-+-+-+-+-+-+
//	                           |
//	                           | <-------------------- PathKP={FromTree:B, FromSlot:6,
//	                           |                               ToAddr:0x03, ToGen:5, ToLvl:1}
//	                        +[0x03]-------------+
//	                        | lvl=1 gen=5 own=A |
//	                        +-+-+-+-+-+-+-+-+-+-+
//	                        |0|1|2|3|4|5|6|7|8|9|
//	                        +-+-+-+-+-+-+-+-+-+-+
//	                               |
//	                               | <---------------- PathKP={FromTree:A, FromSlot:3,
//	                               |                           ToAddr:0x04, ToGen:2, ToLvl:0}
//	                             +[0x04]-------------+
//	                             | lvl=0 gen=2 own=A |
//	                             +-+-+-+-+-+-+-+-+-+-+
//	                             |0|1|2|3|4|5|6|7|8|9|
//	                             +-+-+-+-+-+-+-+-+-+-+
//	                                |
//	                                | <--------------- PathItem={FromTree:A, FromSlot:1}
//	                                |
//	                              [item]
type Path []PathElem

// A PathElem is either a PathRoot, a PathKP, or a PathItem.
type PathElem interface {
	isPathElem()
}

type PathRoot struct {
	Tree Tree
	// It should be no surprise that these 4 members mimic the 4
	// members of a 'RawTree'.
	TreeID       btrfsprim.ObjID
	ToAddr       btrfsvol.LogicalAddr
	ToGeneration btrfsprim.Generation
	ToLevel      uint8
}

func (PathRoot) isPathElem() {}

type PathKP struct {
	// From the containing Node.
	FromTree btrfsprim.ObjID
	FromSlot int
	// From the KP itself.
	ToAddr       btrfsvol.LogicalAddr
	ToGeneration btrfsprim.Generation
	ToMinKey     btrfsprim.Key
	// From the structure of the tree.
	ToMaxKey btrfsprim.Key
	ToLevel  uint8
}

func (PathKP) isPathElem() {}

type PathItem struct {
	// From the containing Node.
	FromTree btrfsprim.ObjID
	FromSlot int
	// From the Item itself.
	ToKey btrfsprim.Key
}

func (PathItem) isPathElem() {}

func (path Path) String() string {
	if len(path) == 0 {
		return "(empty-path)"
	}
	var ret strings.Builder
	for _, elem := range path {
		switch elem := elem.(type) {
		case PathRoot:
			fmt.Fprintf(&ret, "%s->node:%d@%v",
				elem.TreeID.Format(btrfsprim.ROOT_TREE_OBJECTID),
				elem.ToLevel, elem.ToAddr)
		case PathKP:
			fmt.Fprintf(&ret, "[%d]->node:%d@%v",
				elem.FromSlot,
				elem.ToLevel, elem.ToAddr)
		case PathItem:
			// fmt.Fprintf(&ret, "[%d]->item:%v",
			// 	elem.FromSlot,
			// 	elem.ToKey)
			fmt.Fprintf(&ret, "[%d]",
				elem.FromSlot)
		default:
			panic(fmt.Errorf("should not happen: unexpected PathElem type: %T", elem))
		}
	}
	return ret.String()
}

// NodeExpectations returns the address to read and the expectations
// to have when reading the node pointed to by this Path.
//
// `ok` is false if the path is empty or if this Path points to an
// item rather than a node.
func (path Path) NodeExpectations(ctx context.Context, failOpen bool) (_ btrfsvol.LogicalAddr, _ NodeExpectations, ok bool) {
	if len(path) == 0 {
		return 0, NodeExpectations{}, false
	}
	firstElem, ok := path[0].(PathRoot)
	if !ok {
		panic(fmt.Errorf("should not happen: first PathElem is not PathRoot: %T", path[0]))
	}
	switch lastElem := path[len(path)-1].(type) {
	case PathRoot:
		return lastElem.ToAddr, NodeExpectations{
			LAddr:      containers.OptionalValue(lastElem.ToAddr),
			Level:      containers.OptionalValue(lastElem.ToLevel),
			Generation: containers.OptionalValue(lastElem.ToGeneration),
			Owner: func(owner btrfsprim.ObjID, gen btrfsprim.Generation) error {
				return firstElem.Tree.TreeCheckOwner(ctx, failOpen, owner, gen)
			},
			MinItem: containers.OptionalValue(btrfsprim.Key{}),
			MaxItem: containers.OptionalValue(btrfsprim.MaxKey),
		}, true
	case PathKP:
		return lastElem.ToAddr, NodeExpectations{
			LAddr:      containers.OptionalValue(lastElem.ToAddr),
			Level:      containers.OptionalValue(lastElem.ToLevel),
			Generation: containers.OptionalValue(lastElem.ToGeneration),
			Owner: func(owner btrfsprim.ObjID, gen btrfsprim.Generation) error {
				return firstElem.Tree.TreeCheckOwner(ctx, failOpen, owner, gen)
			},
			MinItem: containers.OptionalValue(lastElem.ToMinKey),
			MaxItem: containers.OptionalValue(lastElem.ToMaxKey),
		}, true
	case PathItem:
		return 0, NodeExpectations{}, false
	default:
		panic(fmt.Errorf("should not happen: unexpected PathElem type: %T", lastElem))
	}
}

func (path Path) Parent() Path {
	return path[:len(path)-1]
}
