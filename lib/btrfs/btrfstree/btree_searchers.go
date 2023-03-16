// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree

import (
	"fmt"
	"strings"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

type SearchItemType int8

const (
	ItemTypeAny SearchItemType = iota
	ItemTypeExact
)

type SearchOffset int8

const (
	OffsetAny SearchOffset = iota
	OffsetExact
	OffsetRange // .Search behaves same as OffsetAny (TODO?)
	OffsetName  // .Search behaves same as OffsetAny
)

// Search is a fairly generic and reusable implementation of
// TreeSearcher.
type Search struct {
	ObjectID btrfsprim.ObjID

	ItemTypeMatching SearchItemType
	ItemType         btrfsprim.ItemType

	// Offset is totally ignored if .ItemTypeMatching=ItemTypeany.
	OffsetMatching SearchOffset
	OffsetLow      uint64 // only for .OffsetMatching==OffsetExact or .OffsetMatching==OffsetRange
	OffsetHigh     uint64 // only for .OffsetMatching==OffsetRange
	OffsetName     string // only for .OffsetMatching==OffsetName
}

var (
	_ containers.Ordered[Search] = Search{}
	_ TreeSearcher               = Search{}
)

// Compare implements containers.Ordered.
func (a Search) Compare(b Search) int {
	if d := containers.NativeCompare(a.ObjectID, b.ObjectID); d != 0 {
		return d
	}
	if d := containers.NativeCompare(a.ItemType, b.ItemType); d != 0 {
		return d
	}
	if d := containers.NativeCompare(a.OffsetMatching, b.OffsetMatching); d != 0 {
		return d
	}
	if d := containers.NativeCompare(a.OffsetLow, b.OffsetLow); d != 0 {
		return d
	}
	if d := containers.NativeCompare(a.OffsetHigh, b.OffsetHigh); d != 0 {
		return d
	}
	if d := containers.NativeCompare(a.OffsetName, b.OffsetName); d != 0 {
		return d
	}
	return 0
}

// String implements fmt.Stringer (and TreeSearcher).
func (o Search) String() string {
	var buf strings.Builder

	fmt.Fprintf(&buf, "(%v ", o.ObjectID)

	switch o.ItemTypeMatching {
	case ItemTypeAny:
		buf.WriteString("? ?)")
		return buf.String()
	case ItemTypeExact:
		fmt.Fprintf(&buf, "%v", o.ItemType)
	default:
		panic(fmt.Errorf("should not happen: ItemTypeMatching=%#v", o.ItemTypeMatching))
	}

	buf.WriteString(" ")

	switch o.OffsetMatching {
	case OffsetAny:
		buf.WriteString("?")
	case OffsetExact:
		fmt.Fprintf(&buf, "%v", o.OffsetLow)
	case OffsetRange:
		fmt.Fprintf(&buf, "%v-%v", o.OffsetLow, o.OffsetHigh)
	case OffsetName:
		fmt.Fprintf(&buf, "name=%q", o.OffsetName)
	default:
		panic(fmt.Errorf("should not happen: OffsetMatching=%#v", o.OffsetMatching))
	}

	buf.WriteString(")")

	return buf.String()
}

// Search implements TreeSearcher.
func (o Search) Search(k btrfsprim.Key, _ uint32) int {
	if d := containers.NativeCompare(o.ObjectID, k.ObjectID); d != 0 {
		return d
	}

	switch o.ItemTypeMatching {
	case ItemTypeAny:
		return 0
	case ItemTypeExact:
		if d := containers.NativeCompare(o.ItemType, k.ItemType); d != 0 {
			return d
		}
	default:
		panic(fmt.Errorf("should not happen: ItemTypeMatching=%#v", o.ItemTypeMatching))
	}

	switch o.OffsetMatching {
	case OffsetAny, OffsetRange, OffsetName:
		return 0
	case OffsetExact:
		return containers.NativeCompare(o.OffsetLow, k.Offset)
	default:
		panic(fmt.Errorf("should not happen: OffsetMatching=%#v", o.OffsetMatching))
	}
}

////////////////////////////////////////////////////////////////////////////////

// SearchObject returns a Search that searches all items belonging to
// a given object.
func SearchObject(objID btrfsprim.ObjID) Search {
	return Search{
		ObjectID:         objID,
		ItemTypeMatching: ItemTypeAny,
	}
}

// SearchExactKey returns a Search that searches for the exact key.
func SearchExactKey(k btrfsprim.Key) Search {
	return Search{
		ObjectID: k.ObjectID,

		ItemTypeMatching: ItemTypeExact,
		ItemType:         k.ItemType,

		OffsetMatching: OffsetExact,
		OffsetLow:      k.Offset,
	}
}

// SearchRootItem returns a Search that searches for the root item for
// the given tree.
func SearchRootItem(treeID btrfsprim.ObjID) Search {
	return Search{
		ObjectID: treeID,

		ItemTypeMatching: ItemTypeExact,
		ItemType:         btrfsprim.ROOT_ITEM_KEY,

		OffsetMatching: OffsetAny,
	}
}

type csumSearcher struct {
	laddr   btrfsvol.LogicalAddr
	algSize int
}

func (s csumSearcher) String() string { return fmt.Sprintf("csum for laddr=%v", s.laddr) }
func (s csumSearcher) Search(key btrfsprim.Key, size uint32) int {
	if d := containers.NativeCompare(btrfsprim.EXTENT_CSUM_OBJECTID, key.ObjectID); d != 0 {
		return d
	}
	if d := containers.NativeCompare(btrfsprim.EXTENT_CSUM_KEY, key.ItemType); d != 0 {
		return d
	}
	itemBeg := btrfsvol.LogicalAddr(key.Offset)
	numSums := int64(size) / int64(s.algSize)
	itemEnd := itemBeg + btrfsvol.LogicalAddr(numSums*btrfssum.BlockSize)
	switch {
	case itemEnd <= s.laddr:
		return 1
	case s.laddr < itemBeg:
		return -1
	default:
		return 0
	}
}

// SearchCSum returns a TreeSearcher that searches for a csum-run
// containing the csum for a given LogicalAddress.
func SearchCSum(laddr btrfsvol.LogicalAddr, algSize int) TreeSearcher {
	return csumSearcher{
		laddr:   laddr,
		algSize: algSize,
	}
}
