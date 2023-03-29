// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree

import (
	"fmt"

	"github.com/datawire/dlib/derror"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

type NodeExpectations struct {
	LAddr containers.Optional[btrfsvol.LogicalAddr]
	// Things knowable from the parent.
	Level      containers.Optional[uint8]
	Generation containers.Optional[btrfsprim.Generation]
	Owner      func(btrfsprim.ObjID, btrfsprim.Generation) error
	MinItem    containers.Optional[btrfsprim.Key]
	// Things knowable from the structure of the tree.
	MaxItem containers.Optional[btrfsprim.Key]
}

func (exp NodeExpectations) Check(node *Node) error {
	var errs derror.MultiError
	if exp.LAddr.OK && node.Head.Addr != exp.LAddr.Val {
		errs = append(errs, fmt.Errorf("read from laddr=%v but claims to be at laddr=%v",
			exp.LAddr.Val, node.Head.Addr))
	}
	if exp.Level.OK && node.Head.Level != exp.Level.Val {
		errs = append(errs, fmt.Errorf("expected level=%v but claims to be level=%v",
			exp.Level.Val, node.Head.Level))
	}
	if exp.Generation.OK && node.Head.Generation != exp.Generation.Val {
		errs = append(errs, fmt.Errorf("expected generation=%v but claims to be generation=%v",
			exp.Generation.Val, node.Head.Generation))
	}
	if exp.Owner != nil {
		if err := exp.Owner(node.Head.Owner, node.Head.Generation); err != nil {
			errs = append(errs, err)
		}
	}
	if node.Head.NumItems == 0 {
		errs = append(errs, fmt.Errorf("has no items"))
	} else {
		if minItem, _ := node.MinItem(); exp.MinItem.OK && exp.MinItem.Val.Compare(minItem) > 0 {
			errs = append(errs, fmt.Errorf("expected minItem>=%v but node has minItem=%v",
				exp.MinItem, minItem))
		}
		if maxItem, _ := node.MaxItem(); exp.MaxItem.OK && exp.MaxItem.Val.Compare(maxItem) < 0 {
			errs = append(errs, fmt.Errorf("expected maxItem<=%v but node has maxItem=%v",
				exp.MaxItem, maxItem))
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}
