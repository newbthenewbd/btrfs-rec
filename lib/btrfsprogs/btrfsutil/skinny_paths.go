// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
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
	fatItems *containers.LRUCache[skinnyItem, btrfstree.TreePathElem]
}

func (a *SkinnyPathArena) init() {
	if a.fatRoots == nil {
		a.fatRoots = make(map[btrfsvol.LogicalAddr]btrfstree.TreePathElem)
		// This cache size is sorta arbitrary.  At first I figured
		// "let's allow 1GB of cached items", and figured 67bytes per
		// item, that's about 16M items.  But with overhead of the
		// LRUCache, it's actually a lot higher than that.  So then I
		// cut it to .5M, and that cut my total memory use to ~8GB,
		// which is a good number for me.  Then I tought it to do a
		// better job of recovering trees, and so the memory grew, and I
		// cut it to 64K.  Then to 8K.  Then grew it to 128K.
		a.fatItems = containers.NewLRUCache[skinnyItem, btrfstree.TreePathElem](textui.Tunable(128 * 1024))
	}
}

func (a *SkinnyPathArena) getItem(parent btrfstree.TreePath, itemIdx int) (btrfstree.TreePathElem, error) {
	if itemIdx < 0 {
		panic("should not happen")
	}

	a.init()

	ret, ok := a.fatItems.Get(skinnyItem{
		Node: parent.Node(-1).ToNodeAddr,
		Item: itemIdx,
	})
	if ok {
		return ret, nil
	}

	node, err := btrfstree.ReadNode(a.FS, a.SB, parent.Node(-1).ToNodeAddr, btrfstree.NodeExpectations{})
	if err != nil {
		return btrfstree.TreePathElem{}, err
	}
	if node.Data.Head.Level > 0 {
		if itemIdx >= len(node.Data.BodyInternal) {
			panic("should not happen")
		}
		for i, item := range node.Data.BodyInternal {
			toMaxKey := parent.Node(-1).ToMaxKey
			if i+1 < len(node.Data.BodyInternal) {
				toMaxKey = node.Data.BodyInternal[i+1].Key.Mm()
			}
			elem := btrfstree.TreePathElem{
				FromTree:         node.Data.Head.Owner,
				FromItemIdx:      i,
				ToNodeAddr:       item.BlockPtr,
				ToNodeGeneration: item.Generation,
				ToNodeLevel:      node.Data.Head.Level - 1,
				ToKey:            item.Key,
				ToMaxKey:         toMaxKey,
			}
			a.fatItems.Add(skinnyItem{Node: parent.Node(-1).ToNodeAddr, Item: i}, elem)
			if i == itemIdx {
				ret = elem
			}
		}
	} else {
		if itemIdx >= len(node.Data.BodyLeaf) {
			panic("should not happen")
		}
		for i, item := range node.Data.BodyLeaf {
			elem := btrfstree.TreePathElem{
				FromTree:    node.Data.Head.Owner,
				FromItemIdx: i,
				ToKey:       item.Key,
				ToMaxKey:    item.Key,
			}
			a.fatItems.Add(skinnyItem{Node: parent.Node(-1).ToNodeAddr, Item: i}, elem)
			if i == itemIdx {
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
			a.fatItems.Add(skinnyItem{Node: prevNode, Item: elem.FromItemIdx}, elem)
			ret.Items = append(ret.Items, elem.FromItemIdx)
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

	for _, itemIdx := range skinny.Items {
		elem, err := a.getItem(ret, itemIdx)
		if err != nil {
			panic(err)
		}
		ret = append(ret, elem)
	}

	return ret
}
