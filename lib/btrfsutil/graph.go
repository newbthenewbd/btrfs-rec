// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type GraphNode struct {
	Addr       btrfsvol.LogicalAddr
	Level      uint8
	Generation btrfsprim.Generation
	Owner      btrfsprim.ObjID
	Items      []btrfsprim.Key
}

func (n GraphNode) NumItems(g Graph) int {
	switch n.Level {
	case 0:
		return len(n.Items)
	default:
		return len(g.EdgesFrom[n.Addr])
	}
}

func (n GraphNode) MinItem(g Graph) btrfsprim.Key {
	if n.NumItems(g) == 0 {
		return btrfsprim.Key{}
	}
	switch n.Level {
	case 0:
		return n.Items[0]
	default:
		return g.EdgesFrom[n.Addr][0].ToKey
	}
}

func (n GraphNode) MaxItem(g Graph) btrfsprim.Key {
	if n.NumItems(g) == 0 {
		return btrfsprim.Key{}
	}
	switch n.Level {
	case 0:
		return n.Items[len(n.Items)-1]
	default:
		return g.EdgesFrom[n.Addr][len(g.EdgesFrom[n.Addr])-1].ToKey
	}
}

func (n GraphNode) String() string {
	if reflect.ValueOf(n).IsZero() {
		return "{}"
	}
	return fmt.Sprintf(`{lvl:%v, gen:%v, tree:%v, cnt:%v}`,
		n.Level, n.Generation, n.Owner, len(n.Items))
}

type GraphEdge struct {
	// It is invalid for both 'FromRoot' and 'FromNode' to be
	// non-zero.  If both are zero, then the GraphEdge is from the
	// superblock.
	FromRoot btrfsvol.LogicalAddr
	FromNode btrfsvol.LogicalAddr
	FromSlot int // only valid if one of FromRoot or FromNode is non-zero

	FromTree btrfsprim.ObjID

	ToNode       btrfsvol.LogicalAddr
	ToLevel      uint8
	ToKey        btrfsprim.Key
	ToGeneration btrfsprim.Generation
}

func (kp GraphEdge) String() string {
	var from string
	switch {
	case kp.FromRoot != 0:
		from = fmt.Sprintf("root@%v[%d]:%v",
			kp.FromRoot, kp.FromSlot, kp.FromTree)
	case kp.FromNode != 0:
		from = fmt.Sprintf("{node:%v, tree:%v}[%d]",
			kp.FromNode, kp.FromTree, kp.FromSlot)
	default:
		from = fmt.Sprintf("superblock:%v", kp.FromTree)
	}
	return fmt.Sprintf(`%s -> {n:%v,l:%v,g:%v,k:(%v,%v,%v)}`,
		from,
		kp.ToNode, kp.ToLevel, kp.ToGeneration,
		kp.ToKey.ObjectID,
		kp.ToKey.ItemType,
		kp.ToKey.Offset)
}

type Graph struct {
	Nodes     map[btrfsvol.LogicalAddr]GraphNode
	BadNodes  map[btrfsvol.LogicalAddr]error
	EdgesFrom map[btrfsvol.LogicalAddr][]*GraphEdge
	EdgesTo   map[btrfsvol.LogicalAddr][]*GraphEdge
}

func (g Graph) insertEdge(ptr *GraphEdge) {
	if ptr.ToNode == 0 {
		panic("kp.ToNode should not be zero")
	}
	if ptr.FromRoot != 0 && ptr.FromNode != 0 {
		panic("kp.FromRoot and kp.FromNode should not both be set")
	}
	if (ptr.FromRoot == 0 && ptr.FromNode == 0) && ptr.FromSlot != 0 {
		panic("kp.FromSlot should only be set if either kp.FromRoot or kp.FromSlot is set")
	}
	g.EdgesFrom[ptr.FromNode] = append(g.EdgesFrom[ptr.FromNode], ptr)
	g.EdgesTo[ptr.ToNode] = append(g.EdgesTo[ptr.ToNode], ptr)
}

func (g Graph) insertTreeRoot(ctx context.Context, sb btrfstree.Superblock, treeID btrfsprim.ObjID) {
	treeInfo, err := btrfstree.LookupTreeRoot(ctx, nil, sb, treeID)
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
	g.insertEdge(&GraphEdge{
		FromTree:     treeID,
		ToNode:       treeInfo.RootNode,
		ToLevel:      treeInfo.Level,
		ToGeneration: treeInfo.Generation,
	})
}

func NewGraph(ctx context.Context, sb btrfstree.Superblock) Graph {
	g := Graph{
		Nodes:     make(map[btrfsvol.LogicalAddr]GraphNode),
		BadNodes:  make(map[btrfsvol.LogicalAddr]error),
		EdgesFrom: make(map[btrfsvol.LogicalAddr][]*GraphEdge),
		EdgesTo:   make(map[btrfsvol.LogicalAddr][]*GraphEdge),
	}

	// These 4 trees are mentioned directly in the superblock, so
	// they are always seen.
	g.insertTreeRoot(ctx, sb, btrfsprim.ROOT_TREE_OBJECTID)
	g.insertTreeRoot(ctx, sb, btrfsprim.CHUNK_TREE_OBJECTID)
	g.insertTreeRoot(ctx, sb, btrfsprim.TREE_LOG_OBJECTID)
	g.insertTreeRoot(ctx, sb, btrfsprim.BLOCK_GROUP_TREE_OBJECTID)

	return g
}

func (g Graph) InsertNode(node *btrfstree.Node) {
	nodeData := GraphNode{
		Addr:       node.Head.Addr,
		Level:      node.Head.Level,
		Generation: node.Head.Generation,
		Owner:      node.Head.Owner,
	}

	if node.Head.Level == 0 {
		cnt := 0
		for _, item := range node.BodyLeaf {
			if _, ok := item.Body.(*btrfsitem.Root); ok {
				cnt++
			}
		}
		kps := make([]GraphEdge, 0, cnt)
		keys := make([]btrfsprim.Key, len(node.BodyLeaf))
		nodeData.Items = keys
		g.Nodes[node.Head.Addr] = nodeData
		for i, item := range node.BodyLeaf {
			keys[i] = item.Key
			if itemBody, ok := item.Body.(*btrfsitem.Root); ok {
				kps = append(kps, GraphEdge{
					FromRoot:     node.Head.Addr,
					FromSlot:     i,
					FromTree:     item.Key.ObjectID,
					ToNode:       itemBody.ByteNr,
					ToLevel:      itemBody.Level,
					ToGeneration: itemBody.Generation,
				})
				g.insertEdge(&kps[len(kps)-1])
			}
		}
	} else {
		g.Nodes[node.Head.Addr] = nodeData
		kps := make([]GraphEdge, len(node.BodyInterior))
		for i, kp := range node.BodyInterior {
			kps[i] = GraphEdge{
				FromNode:     node.Head.Addr,
				FromSlot:     i,
				FromTree:     node.Head.Owner,
				ToNode:       kp.BlockPtr,
				ToLevel:      node.Head.Level - 1,
				ToKey:        kp.Key,
				ToGeneration: kp.Generation,
			}
			g.insertEdge(&kps[i])
		}
	}
}

func (g Graph) FinalCheck(ctx context.Context, fs btrfstree.NodeSource) error {
	var stats textui.Portion[int]

	dlog.Info(ctx, "Checking keypointers for dead-ends...")
	progressWriter := textui.NewProgress[textui.Portion[int]](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
	stats.D = len(g.EdgesTo)
	progressWriter.Set(stats)
	for laddr := range g.EdgesTo {
		if _, ok := g.Nodes[laddr]; !ok {
			node, err := fs.AcquireNode(ctx, laddr, btrfstree.NodeExpectations{
				LAddr: containers.OptionalValue(laddr),
			})
			fs.ReleaseNode(node)
			if err == nil {
				progressWriter.Done()
				return fmt.Errorf("node@%v exists but was not in node scan results", laddr)
			}
			g.BadNodes[laddr] = err
		}
		stats.N++
		progressWriter.Set(stats)
	}
	progressWriter.Done()
	dlog.Info(ctx, "... done checking keypointers")

	dlog.Info(ctx, "Checking for btree loops...")
	stats.D = len(g.Nodes)
	stats.N = 0
	progressWriter = textui.NewProgress[textui.Portion[int]](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
	progressWriter.Set(stats)
	visited := make(containers.Set[btrfsvol.LogicalAddr], len(g.Nodes))
	numLoops := 0
	var checkNode func(node btrfsvol.LogicalAddr, stack []btrfsvol.LogicalAddr)
	checkNode = func(node btrfsvol.LogicalAddr, stack []btrfsvol.LogicalAddr) {
		defer func() {
			stats.N = len(visited)
			progressWriter.Set(stats)
		}()
		if visited.Has(node) {
			return
		}
		if slices.Contains(node, stack) {
			numLoops++
			dlog.Error(ctx, "loop:")
			for _, line := range g.renderLoop(append(stack, node)) {
				dlog.Errorf(ctx, "    %s", line)
			}
			return
		}
		stack = append(stack, node)
		for _, kp := range g.EdgesTo[node] {
			checkNode(kp.FromNode, stack)
		}
		visited.Insert(node)
	}
	for _, node := range maps.SortedKeys(g.Nodes) {
		checkNode(node, nil)
	}
	progressWriter.Done()
	if numLoops > 0 {
		return fmt.Errorf("%d btree loops", numLoops)
	}
	dlog.Info(ctx, "... done checking for loops")

	return nil
}
