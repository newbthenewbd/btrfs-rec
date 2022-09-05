// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	iofs "io/fs"
	"strings"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

func VisualizeNodes(ctx context.Context, out io.Writer, fs *btrfs.FS, nodeScanResults btrfsinspect.ScanDevicesResult) error {
	uuidMap, err := buildUUIDMap(ctx, fs, nodeScanResults)
	if err != nil {
		return err
	}

	nfs := &RebuiltTrees{
		inner:   fs,
		uuidMap: uuidMap,
	}

	orphanedNodes, _, treeAncestors, err := classifyNodes(ctx, nfs, nodeScanResults)
	if err != nil {
		return err
	}

	uuidMap.considerAncestors(ctx, treeAncestors)

	////////////////////////////////////////////////////////////////////////////////////////////

	nodes := make(map[btrfsprim.ObjID]containers.Set[string])
	edges := make(containers.Set[string])
	visitedNodes := make(containers.Set[btrfsvol.LogicalAddr])
	var isOrphan bool

	nodeHandler := func(path btrfstree.TreePath, nodeRef *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node], err error) error {
		addr := path.Node(-1).ToNodeAddr

		// Node
		var treeID btrfsprim.ObjID
		var nodeStr string
		if err != nil && (errors.Is(err, btrfstree.ErrNotANode) || errors.As(err, new(*btrfstree.IOError))) {
			treeID = 0
			nodeStr = fmt.Sprintf(`n%d [shape=star label="%v"]`, addr, addr)
		} else {
			treeID = nodeRef.Data.Head.Owner
			var buf strings.Builder
			fmt.Fprintf(&buf, `n%d [shape=record label="%v\ngen=%v\nlvl=%v|`,
				addr,
				nodeRef.Data.Head.Addr,
				nodeRef.Data.Head.Generation,
				nodeRef.Data.Head.Level)
			if nodeRef.Data.Head.Level == 0 {
				for i, item := range nodeRef.Data.BodyLeaf {
					sep := "|"
					if i == 0 {
						sep = "{"
					}
					fmt.Fprintf(&buf, "%s<p%d>%d: (%d,%v,%d)",
						sep, i, i,
						item.Key.ObjectID,
						item.Key.ItemType,
						item.Key.Offset)
				}
			} else {
				for i, ptr := range nodeRef.Data.BodyInternal {
					sep := "|"
					if i == 0 {
						sep = "{"
					}
					fmt.Fprintf(&buf, "%s<p%d>%d: (%d,%v,%d) gen=%v",
						sep, i, i,
						ptr.Key.ObjectID,
						ptr.Key.ItemType,
						ptr.Key.Offset,
						ptr.Generation)
				}
			}
			buf.WriteString(`}"]`)
			nodeStr = buf.String()
		}
		if _, ok := nodes[treeID]; !ok {
			nodes[treeID] = make(containers.Set[string])
			nodes[treeID].Insert(fmt.Sprintf(`t%d [label="%s"]`, treeID, html.EscapeString(treeID.String())))
		}
		nodes[treeID].Insert(nodeStr)

		// Edge
		var edge strings.Builder
		if len(path) == 1 {
			if isOrphan {
				edge.WriteString("orphanRoot")
			} else {
				fmt.Fprintf(&edge, "t%d", path[0].FromTree)
			}
		} else {
			fmt.Fprintf(&edge, "n%d:p%d", path.Node(-2).ToNodeAddr, path.Node(-1).FromItemIdx)
		}
		fmt.Fprintf(&edge, " -> n%d", addr)
		if err != nil {
			fmt.Fprintf(&edge, ` [color=red label="%s"]`, html.EscapeString(err.Error()))
		}
		edges.Insert(edge.String())

		// Return
		if visitedNodes.Has(addr) {
			return iofs.SkipDir
		}
		visitedNodes.Insert(addr)
		return err
	}

	walkHandler := btrfstree.TreeWalkHandler{
		Node: func(path btrfstree.TreePath, nodeRef *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
			return nodeHandler(path, nodeRef, nil)
		},
		BadNode: func(path btrfstree.TreePath, nodeRef *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node], err error) error {
			return nodeHandler(path, nodeRef, err)
		},
	}

	btrfsutil.WalkAllTrees(ctx, nfs, btrfsutil.WalkAllTreesHandler{
		TreeWalkHandler: walkHandler,
		Err: func(err *btrfsutil.WalkError) {
			// do nothing
		},
	})
	isOrphan = true
	for _, potentialRoot := range maps.SortedKeys(orphanedNodes) {
		walkFromNode(ctx, nfs, potentialRoot,
			func(err *btrfstree.TreeError) {
				// do nothing
			},
			walkHandler,
		)
	}

	////////////////////////////////////////////////////////////////////////////////////////////

	fmt.Fprintln(out, "digraph FS {")
	for _, treeID := range maps.SortedKeys(nodes) {
		fmt.Fprintf(out, "  subgraph cluster_%d {\n", treeID)
		for _, node := range maps.SortedKeys(nodes[treeID]) {
			fmt.Fprintf(out, "    %s;\n", node)
		}
		fmt.Fprintln(out, "  }")
	}
	for _, edge := range maps.SortedKeys(edges) {
		fmt.Fprintf(out, "  %s;\n", edge)
	}
	fmt.Fprintln(out, "}")

	return nil
}
