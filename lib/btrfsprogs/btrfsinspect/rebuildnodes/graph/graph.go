// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package graph

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type Node struct {
	Level      uint8
	Generation btrfsprim.Generation
	Owner      btrfsprim.ObjID
	NumItems   uint32
	MinItem    btrfsprim.Key
	MaxItem    btrfsprim.Key
}

type Edge struct {
	FromTree btrfsprim.ObjID
	FromNode btrfsvol.LogicalAddr
	FromItem int

	ToNode       btrfsvol.LogicalAddr
	ToLevel      uint8
	ToKey        btrfsprim.Key
	ToGeneration btrfsprim.Generation
}

func (kp Edge) String() string {
	return fmt.Sprintf(`{t:%v,n:%v}[%d]->{n:%v,l:%v,g:%v,k:(%d,%v,%d)}`,
		kp.FromTree, kp.FromNode, kp.FromItem,
		kp.ToNode, kp.ToLevel, kp.ToGeneration,
		kp.ToKey.ObjectID,
		kp.ToKey.ItemType,
		kp.ToKey.Offset)
}

type Graph struct {
	Nodes     map[btrfsvol.LogicalAddr]Node
	BadNodes  map[btrfsvol.LogicalAddr]error
	EdgesFrom map[btrfsvol.LogicalAddr][]*Edge
	EdgesTo   map[btrfsvol.LogicalAddr][]*Edge
}

func (g Graph) insertEdge(kp Edge) {
	ptr := &kp
	if kp.ToNode == 0 {
		panic("kp.ToNode should not be zero")
	}
	g.EdgesFrom[kp.FromNode] = append(g.EdgesFrom[kp.FromNode], ptr)
	g.EdgesTo[kp.ToNode] = append(g.EdgesTo[kp.ToNode], ptr)
}

func (g Graph) insertTreeRoot(sb btrfstree.Superblock, treeID btrfsprim.ObjID) {
	treeInfo, err := btrfstree.LookupTreeRoot(nil, sb, treeID)
	if err != nil {
		// This shouldn't ever happen for treeIDs that are
		// mentioned directly in the superblock; which are the
		// only trees for which we should call
		// .insertTreeRoot().
		panic(fmt.Errorf("LookupTreeRoot(%v): %w", treeID, err))
	}
	if treeInfo.RootNode == 0 {
		return
	}
	g.insertEdge(Edge{
		FromTree:     treeID,
		ToNode:       treeInfo.RootNode,
		ToLevel:      treeInfo.Level,
		ToGeneration: treeInfo.Generation,
	})
}

func New(sb btrfstree.Superblock) *Graph {
	g := &Graph{
		Nodes:     make(map[btrfsvol.LogicalAddr]Node),
		BadNodes:  make(map[btrfsvol.LogicalAddr]error),
		EdgesFrom: make(map[btrfsvol.LogicalAddr][]*Edge),
		EdgesTo:   make(map[btrfsvol.LogicalAddr][]*Edge),
	}

	// These 4 trees are mentioned directly in the superblock, so
	// they are always seen.
	g.insertTreeRoot(sb, btrfsprim.ROOT_TREE_OBJECTID)
	g.insertTreeRoot(sb, btrfsprim.CHUNK_TREE_OBJECTID)
	g.insertTreeRoot(sb, btrfsprim.TREE_LOG_OBJECTID)
	g.insertTreeRoot(sb, btrfsprim.BLOCK_GROUP_TREE_OBJECTID)

	return g
}

func (g *Graph) InsertNode(nodeRef *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) {
	for _, item := range nodeRef.Data.BodyLeaf {
		switch itemBody := item.Body.(type) {
		case btrfsitem.Root:
			g.insertEdge(Edge{
				FromTree:     item.Key.ObjectID,
				ToNode:       itemBody.ByteNr,
				ToLevel:      itemBody.Level,
				ToGeneration: itemBody.Generation,
			})
		}
	}

	g.Nodes[nodeRef.Addr] = Node{
		Level:      nodeRef.Data.Head.Level,
		Generation: nodeRef.Data.Head.Generation,
		Owner:      nodeRef.Data.Head.Owner,
		NumItems:   nodeRef.Data.Head.NumItems,
		MinItem:    discardOK(nodeRef.Data.MinItem()),
		MaxItem:    discardOK(nodeRef.Data.MaxItem()),
	}
	for i, kp := range nodeRef.Data.BodyInternal {
		g.insertEdge(Edge{
			FromTree:     nodeRef.Data.Head.Owner,
			FromNode:     nodeRef.Addr,
			FromItem:     i,
			ToNode:       kp.BlockPtr,
			ToLevel:      nodeRef.Data.Head.Level - 1,
			ToKey:        kp.Key,
			ToGeneration: kp.Generation,
		})
	}
}

func (g *Graph) FinalCheck(fs diskio.File[btrfsvol.LogicalAddr], sb btrfstree.Superblock, progress func(n, d int)) error {
	total := len(g.EdgesTo)
	done := 0
	progress(done, total)
	for laddr := range g.EdgesTo {
		if _, ok := g.Nodes[laddr]; !ok {
			_, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, sb, laddr, btrfstree.NodeExpectations{
				LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: laddr},
			})
			if err == nil {
				return fmt.Errorf("node@%v exists but was not in node scan results", laddr)
			}
			g.BadNodes[laddr] = err
		}
		done++
		progress(done, total)
	}
	return nil
}

func discardOK[T any](val T, _ bool) T {
	return val
}
