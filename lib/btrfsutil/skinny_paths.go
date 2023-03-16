// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type skinnyItem struct {
	Node btrfsvol.LogicalAddr
	Item int
}

type SkinnyPath struct {
	Root  btrfsvol.LogicalAddr
	Items []int
}

type SkinnyPathArena struct {
	FS diskio.File[btrfsvol.LogicalAddr]
	SB btrfstree.Superblock

	fatRoots map[btrfsvol.LogicalAddr]btrfstree.TreePathElem
	fatItems containers.ARCache[skinnyItem, btrfstree.TreePathElem]
}

func (a *SkinnyPathArena) init() {
	if a.fatRoots == nil {
		a.fatRoots = make(map[btrfsvol.LogicalAddr]btrfstree.TreePathElem)
		a.fatItems.MaxLen = textui.Tunable(128 * 1024)
	}
}

func (a *SkinnyPathArena) getItem(parent btrfstree.TreePath, itemSlot int) (btrfstree.TreePathElem, error) {
	if itemSlot < 0 {
		panic("should not happen")
	}

	a.init()

	ret, ok := a.fatItems.Load(skinnyItem{
		Node: parent.Node(-1).ToNodeAddr,
		Item: itemSlot,
	})
	if ok {
		return ret, nil
	}

	node, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](a.FS, a.SB, parent.Node(-1).ToNodeAddr, btrfstree.NodeExpectations{})
	defer node.Free()
	if err != nil {
		return btrfstree.TreePathElem{}, err
	}
	if node.Head.Level > 0 {
		if itemSlot >= len(node.BodyInterior) {
			panic("should not happen")
		}
		for i, item := range node.BodyInterior {
			toMaxKey := parent.Node(-1).ToMaxKey
			if i+1 < len(node.BodyInterior) {
				toMaxKey = node.BodyInterior[i+1].Key.Mm()
			}
			elem := btrfstree.TreePathElem{
				FromTree:         node.Head.Owner,
				FromItemSlot:     i,
				ToNodeAddr:       item.BlockPtr,
				ToNodeGeneration: item.Generation,
				ToNodeLevel:      node.Head.Level - 1,
				ToKey:            item.Key,
				ToMaxKey:         toMaxKey,
			}
			a.fatItems.Store(skinnyItem{Node: parent.Node(-1).ToNodeAddr, Item: i}, elem)
			if i == itemSlot {
				ret = elem
			}
		}
	} else {
		if itemSlot >= len(node.BodyLeaf) {
			panic("should not happen")
		}
		for i, item := range node.BodyLeaf {
			elem := btrfstree.TreePathElem{
				FromTree:     node.Head.Owner,
				FromItemSlot: i,
				ToKey:        item.Key,
				ToMaxKey:     item.Key,
			}
			a.fatItems.Store(skinnyItem{Node: parent.Node(-1).ToNodeAddr, Item: i}, elem)
			if i == itemSlot {
				ret = elem
			}
		}
	}

	return ret, nil
}

func (a *SkinnyPathArena) Deflate(fat btrfstree.TreePath) SkinnyPath {
	a.init()

	var ret SkinnyPath

	var prevNode btrfsvol.LogicalAddr
	for i, elem := range fat {
		if i == 0 {
			a.fatRoots[elem.ToNodeAddr] = elem
			ret.Root = elem.ToNodeAddr
		} else {
			a.fatItems.Store(skinnyItem{Node: prevNode, Item: elem.FromItemSlot}, elem)
			ret.Items = append(ret.Items, elem.FromItemSlot)
		}
		prevNode = elem.ToNodeAddr
	}
	return ret
}

func (a *SkinnyPathArena) Inflate(skinny SkinnyPath) btrfstree.TreePath {
	a.init()

	ret := make(btrfstree.TreePath, 0, 1+len(skinny.Items))

	root, ok := a.fatRoots[skinny.Root]
	if !ok {
		panic(fmt.Errorf("SkinnyPathArena.Inflate: no stored TreePathElem for root->%v",
			skinny.Root))
	}
	ret = append(ret, root)

	for _, itemSlot := range skinny.Items {
		elem, err := a.getItem(ret, itemSlot)
		if err != nil {
			panic(err)
		}
		ret = append(ret, elem)
	}

	return ret
}
