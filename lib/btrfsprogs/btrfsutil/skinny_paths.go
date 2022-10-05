// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

func maybeSet[K, V comparable](name string, m map[K]V, k K, v V) {
	if other, conflict := m[k]; conflict && other != v {
		panic(fmt.Errorf("conflict: %s %v can't have both %v and %v", name, k, other, v))
	}
	m[k] = v
}

type skinnyKP struct {
	src, dst btrfsvol.LogicalAddr
}

type skinnyItem struct {
	node btrfsvol.LogicalAddr
	item int
}

type SkinnyPathArena struct {
	fatKPs   map[skinnyKP]btrfstree.TreePathElem
	fatItems map[skinnyItem]btrfstree.TreePathElem
}

type SkinnyPath struct {
	Nodes []btrfsvol.LogicalAddr
	Item  int
}

func (a *SkinnyPathArena) init() {
	if a.fatKPs == nil {
		a.fatKPs = make(map[skinnyKP]btrfstree.TreePathElem)
		a.fatItems = make(map[skinnyItem]btrfstree.TreePathElem)
	}
}

func (a *SkinnyPathArena) Deflate(fat btrfstree.TreePath) SkinnyPath {
	a.init()

	var ret SkinnyPath
	ret.Item = -1

	var prevNode btrfsvol.LogicalAddr
	for _, elem := range fat {
		if elem.ToNodeAddr > 0 {
			maybeSet("SkinnyPathArena.fatKPs", a.fatKPs, skinnyKP{
				src: prevNode,
				dst: elem.ToNodeAddr,
			}, elem)
			ret.Nodes = append(ret.Nodes, elem.ToNodeAddr)
		} else {
			maybeSet("SkinnyPathArena.fatItems", a.fatItems, skinnyItem{
				node: prevNode,
				item: elem.FromItemIdx,
			}, elem)
			ret.Item = elem.FromItemIdx
		}
		prevNode = elem.ToNodeAddr
	}

	return ret
}

func (a *SkinnyPathArena) Inflate(skinny SkinnyPath) btrfstree.TreePath {
	a.init()

	var ret btrfstree.TreePath

	var prevNode btrfsvol.LogicalAddr
	for _, node := range skinny.Nodes {
		elem, ok := a.fatKPs[skinnyKP{
			src: prevNode,
			dst: node,
		}]
		if !ok {
			panic(fmt.Errorf("SkinnyPathArena.Inflate: no stored TreePathElem for %v->%v",
				prevNode, node))
		}
		ret = append(ret, elem)
		prevNode = node
	}

	if skinny.Item >= 0 {
		elem, ok := a.fatItems[skinnyItem{
			node: prevNode,
			item: skinny.Item,
		}]
		if !ok {
			panic(fmt.Errorf("SkinnyPathArena.Inflate: no stored TreePathElem for %v[%d]",
				prevNode, skinny.Item))
		}
		ret = append(ret, elem)
	}

	return ret
}
