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

func getCliques(scanData scanResult) map[btrfsprim.ObjID]*containers.Set[btrfsprim.ObjID] {
	cliques := make(map[btrfsprim.ObjID]*containers.Set[btrfsprim.ObjID])

	// UUID map
	lister := newFullAncestorLister(scanData.uuidMap, map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID]{})
	for _, treeID := range maps.SortedKeys(scanData.uuidMap.SeenTrees) {
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

	// node graph
	for _, laddr := range maps.SortedKeys(scanData.nodeGraph.Nodes) {
		clique := ptrTo(make(containers.Set[btrfsprim.ObjID]))
		clique.Insert(scanData.nodeGraph.Nodes[laddr].Owner)
		for _, edge := range scanData.nodeGraph.EdgesTo[laddr] {
			clique.Insert(edge.FromTree)
		}
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

	////////////////////////////////////////////////////////////////////////////////////////////

	dlog.Info(ctx, "Building cliques...")
	cliques := getCliques(*scanData)
	cliqueIDs := make(containers.Set[btrfsprim.ObjID])
	for treeID := range cliques {
		cliqueIDs.Insert(getCliqueID(cliques, treeID))
	}
	dlog.Infof(ctx, "... built %d cliques of %d trees", len(cliqueIDs), len(cliques))

	////////////////////////////////////////////////////////////////////////////////////////////

	dlog.Info(ctx, "Building graphviz graphs...")

	type graph struct {
		nodes    map[btrfsprim.ObjID]containers.Set[string] // keyed by treeID
		badNodes containers.Set[string]
		edges    containers.Set[string]
	}
	graphs := make(map[btrfsprim.ObjID]graph, len(cliques)) // keyed by cliqueID
	for cliqueID := range cliqueIDs {
		graphs[cliqueID] = graph{
			nodes:    make(map[btrfsprim.ObjID]containers.Set[string]),
			badNodes: make(containers.Set[string]),
			edges:    make(containers.Set[string]),
		}
	}

	dlog.Infof(ctx, "... processing %d nodes...", len(scanData.Nodes))

	for _, laddr := range maps.SortedKeys(scanData.Nodes) {
		nodeData := scanData.Nodes[laddr]
		cliqueID := getCliqueID(cliques, nodeData.Owner)
		graph, ok := graphs[cliqueID]
		if !ok {
			panic(cliqueID)
		}
		if graph.nodes[nodeData.Owner] == nil {
			graph.nodes[nodeData.Owner] = make(containers.Set[string])
		}

		var buf strings.Builder
		fmt.Fprintf(&buf, `n%d [shape=record label="{laddr=%v\lgen=%v\llvl=%v\litems=%v\l|{`,
			laddr,
			laddr,
			nodeData.Generation,
			nodeData.Level,
			nodeData.NumItems)
		if nodeData.Level == 0 {
			buf.WriteString("leaf")
		} else {
			for i := uint32(0); i < nodeData.NumItems; i++ {
				if i == 0 {
					fmt.Fprintf(&buf, "<p%[1]d>%[1]d", i)
				} else {
					fmt.Fprintf(&buf, "|<p%[1]d>%[1]d", i)
				}
			}
		}
		buf.WriteString(`}}"]`)
		graph.nodes[nodeData.Owner].Insert(buf.String())

		if len(scanData.EdgesTo[laddr]) == 0 {
			graph.edges.Insert(fmt.Sprintf("orphanRoot -> n%d", laddr))
		}

		graphs[cliqueID] = graph
	}

	dlog.Infof(ctx, "... processing %d bad nodes...", len(scanData.BadNodes))

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
		graph, ok := graphs[cliqueID]
		if !ok {
			panic(cliqueID)
		}
		graph.badNodes.Insert(fmt.Sprintf(`n%d [shape=star color=red label="%v"]`, laddr, laddr))
		graphs[cliqueID] = graph
	}

	numEdges := 0
	for _, kps := range scanData.EdgesFrom {
		numEdges += (len(kps))
	}
	dlog.Infof(ctx, "... processing %d keypointers...", numEdges)

	for _, laddr := range maps.SortedKeys(scanData.EdgesFrom) {
		for _, kp := range scanData.EdgesFrom[laddr] {
			cliqueID := getCliqueID(cliques, kp.FromTree)
			graph, ok := graphs[cliqueID]
			if !ok {
				panic(cliqueID)
			}

			var buf strings.Builder
			if kp.FromNode == 0 {
				if graph.nodes[kp.FromTree] == nil {
					graph.nodes[kp.FromTree] = make(containers.Set[string])
				}
				graph.nodes[kp.FromTree].Insert(fmt.Sprintf(`t%d [label="root of %s"]`, kp.FromTree, html.EscapeString(kp.FromTree.String())))
				fmt.Fprintf(&buf, "t%d", kp.FromTree)
			} else {
				fmt.Fprintf(&buf, "n%d:p%d", kp.FromNode, kp.FromItem)
			}
			fmt.Fprintf(&buf, ` -> n%d`, kp.ToNode)

			var err error
			toNode, ok := scanData.Nodes[kp.ToNode]
			if !ok {
				err = scanData.BadNodes[kp.ToNode]
			} else {
				err = checkNodeExpectations(*kp, toNode)
			}
			if err != nil {
				fmt.Fprintf(&buf, ` [label="key=(%d,%v,%d) gen=%v\l\l%s" color=red]`,
					kp.ToKey.ObjectID,
					kp.ToKey.ItemType,
					kp.ToKey.Offset,
					kp.ToGeneration,
					strings.ReplaceAll(strings.ReplaceAll(html.EscapeString(err.Error())+"\n", "\n", `\l`), `\n`, `\l`))
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
			if _, err := fmt.Fprintf(buf, "rankdir=LR;\n  nodesep=0.1;\n  ranksep=25;\n  splines=line;\n"); err != nil {
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
				if _, err := fmt.Fprintf(buf, "  %s;\n", node); err != nil {
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
