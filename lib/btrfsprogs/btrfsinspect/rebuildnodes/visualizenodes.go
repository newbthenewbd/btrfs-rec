// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"archive/zip"
	"context"
	"fmt"
	"html"
	"io"
	"strings"

	"github.com/datawire/dlib/derror"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

func getCliques(uuidMap uuidMap, treeAncestors map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID]) map[btrfsprim.ObjID]*containers.Set[btrfsprim.ObjID] {
	cliques := make(map[btrfsprim.ObjID]*containers.Set[btrfsprim.ObjID])
	lister := newFullAncestorLister(uuidMap, treeAncestors)
	for _, treeID := range maps.SortedKeys(uuidMap.SeenTrees) {
		clique := ptrTo(make(containers.Set[btrfsprim.ObjID]))
		clique.Insert(treeID)
		clique.InsertFrom(lister.GetFullAncestors(treeID))
		for _, id := range maps.SortedKeys(*clique) {
			if existingClique, ok := cliques[id]; ok {
				clique.InsertFrom(*existingClique)
			}
			cliques[id] = clique
		}
	}
	return cliques
}

func getCliqueID(cliques map[btrfsprim.ObjID]*containers.Set[btrfsprim.ObjID], treeID btrfsprim.ObjID) btrfsprim.ObjID {
	clique, ok := cliques[treeID]
	if !ok {
		panic(fmt.Errorf("tree ID %v was not in .SeenTrees", treeID))
	}
	return maps.SortedKeys(*clique)[0]
}

func VisualizeNodes(ctx context.Context, out io.Writer, fs *btrfs.FS, nodeScanResults btrfsinspect.ScanDevicesResult) error {
	scanData, err := ScanDevices(ctx, fs, nodeScanResults)
	if err != nil {
		return err
	}

	dlog.Info(ctx, "Walking trees to rebuild root items...")
	treeAncestors := getTreeAncestors(ctx, *scanData)
	scanData.considerAncestors(ctx, treeAncestors)

	////////////////////////////////////////////////////////////////////////////////////////////

	cliques := getCliques(scanData.uuidMap, treeAncestors)

	dlog.Info(ctx, "Building graphviz graph...")

	type graph struct {
		nodes    map[btrfsprim.ObjID]containers.Set[string] // keyed by treeID
		badNodes containers.Set[string]
		edges    containers.Set[string]
	}
	graphs := make(map[btrfsprim.ObjID]graph) // keyed by cliqueID

	for _, laddr := range maps.SortedKeys(scanData.Nodes) {
		nodeData := scanData.Nodes[laddr]
		cliqueID := getCliqueID(cliques, nodeData.Owner)
		graph, ok := graphs[cliqueID]
		if !ok {
			graph.nodes = make(map[btrfsprim.ObjID]containers.Set[string])
			graph.badNodes = make(containers.Set[string])
			graph.edges = make(containers.Set[string])
		}
		if graph.nodes[nodeData.Owner] == nil {
			graph.nodes[nodeData.Owner] = make(containers.Set[string])
		}

		var buf strings.Builder
		fmt.Fprintf(&buf, `n%d [shape=record label="%v\ngen=%v\nlvl=%v|{`,
			laddr,
			laddr,
			nodeData.Generation,
			nodeData.Level)
		if nodeData.NumItems == 0 {
			buf.WriteString("(no items)")
		} else {
			for i := uint32(0); i < nodeData.NumItems; i++ {
				if i == 0 {
					fmt.Fprintf(&buf, "<p%[1]d>%[1]d", i)
				} else {
					fmt.Fprintf(&buf, "|<p%[1]d>%[1]d", i)
				}
			}
		}
		buf.WriteString(`}"]`)
		graph.nodes[nodeData.Owner].Insert(buf.String())

		if len(scanData.EdgesTo[laddr]) == 0 {
			graph.edges.Insert(fmt.Sprintf("orphanRoot -> n%d", laddr))
		}

		graphs[cliqueID] = graph
	}

	for _, laddr := range maps.SortedKeys(scanData.BadNodes) {
		cliqueIDs := make(containers.Set[btrfsprim.ObjID])
		for _, edge := range scanData.EdgesTo[laddr] {
			cliqueIDs.Insert(getCliqueID(cliques, edge.FromTree))
		}
		if len(cliqueIDs) != 1 {
			dlog.Errorf(ctx, "couldn't assign bad node@%v to a clique: %v", laddr, maps.SortedKeys(cliqueIDs))
			continue
		}

		cliqueID := cliqueIDs.TakeOne()
		graph := graphs[cliqueID]
		graph.badNodes.Insert(fmt.Sprintf(`n%d [shape=star color=red label="%v"]`, laddr, laddr))
		graphs[cliqueID] = graph
	}

	for _, laddr := range maps.SortedKeys(scanData.EdgesFrom) {
		for _, kp := range scanData.EdgesFrom[laddr] {
			cliqueID := getCliqueID(cliques, kp.FromTree)
			graph := graphs[cliqueID]

			var buf strings.Builder
			if kp.FromNode == 0 {
				graph.nodes[kp.FromTree].Insert(fmt.Sprintf(`t%d [label="%s"]`, kp.FromTree, html.EscapeString(kp.FromTree.String())))
				fmt.Fprintf(&buf, "t%d", kp.FromTree)
			} else {
				fmt.Fprintf(&buf, "n%d", kp.FromNode)
			}
			fmt.Fprintf(&buf, ` -> n%d [label="%d: key=(%d,%v,%d) gen=%v`,
				// dst node
				kp.ToNode,
				// label
				kp.FromItem,
				kp.ToKey.ObjectID,
				kp.ToKey.ItemType,
				kp.ToKey.Offset,
				kp.ToGeneration)
			toNode, ok := scanData.Nodes[kp.ToNode]
			var err error
			if !ok {
				err = scanData.BadNodes[kp.ToNode]
			} else {
				err = checkNodeExpectations(*kp, toNode)
			}
			if err != nil {
				fmt.Fprintf(&buf, `\n\n%s" color=red]`, html.EscapeString(err.Error()))
			} else {
				buf.WriteString(`"]`)
			}

			graph.edges.Insert(buf.String())
			graphs[cliqueID] = graph
		}
	}

	////////////////////////////////////////////////////////////////////////////////////////////

	dlog.Info(ctx, "Writing graphviz output...")

	zw := zip.NewWriter(out)
	for _, cliqueID := range maps.SortedKeys(graphs) {
		if err := func() (err error) {
			graph := graphs[cliqueID]

			buf, err := zw.Create(fmt.Sprintf("%d.dot", cliqueID))
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintf(buf, "strict digraph clique%d {\n", cliqueID); err != nil {
				return err
			}

			for _, treeID := range maps.SortedKeys(graph.nodes) {
				nodes := graph.nodes[treeID]

				if _, err := fmt.Fprintf(buf, "  subgraph cluster_tree%d {\n", treeID); err != nil {
					return err
				}
				for _, node := range maps.SortedKeys(nodes) {
					if _, err := fmt.Fprintf(buf, "    %s;\n", node); err != nil {
						return err
					}
				}
				if _, err := fmt.Fprintln(buf, "  }"); err != nil {
					return err
				}
			}
			for _, node := range maps.SortedKeys(graph.badNodes) {
				if _, err := fmt.Fprintf(buf, "   %s;\n", node); err != nil {
					return err
				}
			}
			for _, edge := range maps.SortedKeys(graph.edges) {
				if _, err := fmt.Fprintf(buf, "  %s;\n", edge); err != nil {
					return err
				}
			}

			if _, err := fmt.Fprintln(buf, "}"); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	if err := zw.Close(); err != nil {
		return err
	}

	dlog.Info(ctx, "... done writing")

	return nil
}

func checkNodeExpectations(kp kpData, toNode nodeData) error {
	var errs derror.MultiError
	if toNode.Level != kp.ToLevel {
		errs = append(errs, fmt.Errorf("node.level=%v != kp.level=%v",
			toNode.Level, kp.ToLevel))
	}
	if toNode.Generation != kp.ToGeneration {
		errs = append(errs, fmt.Errorf("node.generation=%v != kp.generation=%v",
			toNode.Generation, kp.ToGeneration))
	}
	if toNode.NumItems == 0 {
		errs = append(errs, fmt.Errorf("node.num_items=0"))
	} else if toNode.MinItem != kp.ToKey {
		errs = append(errs, fmt.Errorf("node.items[0].key=%v != kp.key=%v",
			toNode.MinItem, kp.ToKey))
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}
