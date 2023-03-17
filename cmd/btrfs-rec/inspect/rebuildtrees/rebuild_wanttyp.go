// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildtrees

import (
	"context"
	"fmt"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

type wantOffsetType int8

const (
	offsetAny = wantOffsetType(iota)
	offsetExact
	offsetRange
	offsetName
)

type Want struct {
	// TODO(lukeshu): Delete the 'Want' type in favor of
	// btrfstree.Search.
	ObjectID   btrfsprim.ObjID
	ItemType   btrfsprim.ItemType
	OffsetType wantOffsetType
	OffsetLow  uint64
	OffsetHigh uint64
	OffsetName string
}

func (a Want) Compare(b Want) int {
	if d := containers.NativeCompare(a.ObjectID, b.ObjectID); d != 0 {
		return d
	}
	if d := containers.NativeCompare(a.ItemType, b.ItemType); d != 0 {
		return d
	}
	if d := containers.NativeCompare(a.OffsetType, b.OffsetType); d != 0 {
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

func (o Want) Key() btrfsprim.Key {
	return btrfsprim.Key{
		ObjectID: o.ObjectID,
		ItemType: o.ItemType,
		Offset:   o.OffsetLow,
	}
}

func wantFromKey(k btrfsprim.Key) Want {
	return Want{
		ObjectID:   k.ObjectID,
		ItemType:   k.ItemType,
		OffsetType: offsetExact,
		OffsetLow:  k.Offset,
	}
}

func (o Want) String() string {
	switch o.OffsetType {
	case offsetAny:
		return fmt.Sprintf("{%v %v ?}", o.ObjectID, o.ItemType)
	case offsetExact:
		return fmt.Sprintf("{%v %v %v}", o.ObjectID, o.ItemType, o.OffsetLow)
	case offsetRange:
		return fmt.Sprintf("{%v %v %v-%v}", o.ObjectID, o.ItemType, o.OffsetLow, o.OffsetHigh)
	case offsetName:
		return fmt.Sprintf("{%v %v name=%q}", o.ObjectID, o.ItemType, o.OffsetName)
	default:
		panic(fmt.Errorf("should not happen: OffsetType=%#v", o.OffsetType))
	}
}

type WantWithTree struct {
	TreeID btrfsprim.ObjID
	Key    Want
}

func (o WantWithTree) String() string {
	return fmt.Sprintf("tree=%v key=%v", o.TreeID, o.Key)
}

const (
	logFieldItemWant = "btrfs.inspect.rebuild-trees.rebuild.want"
	logFieldTreeWant = "btrfs.util.rebuilt-forrest.add-tree.want"
)

func withWant(ctx context.Context, logField, reason string, wantKey WantWithTree) context.Context {
	ctx = dlog.WithField(ctx, logField+".reason", reason)
	ctx = dlog.WithField(ctx, logField+".key", wantKey)
	return ctx
}
