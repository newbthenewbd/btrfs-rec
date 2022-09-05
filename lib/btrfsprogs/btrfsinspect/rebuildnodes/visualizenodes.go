// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"html"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/datawire/dlib/dlog"

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

func ptrTo[T any](x T) *T {
	return &x
}

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

func VisualizeNodes(ctx context.Context, dir string, fs *btrfs.FS, nodeScanResults btrfsinspect.ScanDevicesResult) error {
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

	cliques := getCliques(uuidMap, treeAncestors)

	dlog.Info(ctx, "Building graphviz graph...")

	nodes := make(map[btrfsprim.ObjID]containers.Set[string]) // grouped by treeID
	edges := make(map[btrfsprim.ObjID]containers.Set[string]) // grouped by cliqueID
	visitedNodes := make(containers.Set[btrfsvol.LogicalAddr])
	var isOrphan bool

	nodeHandler := func(path btrfstree.TreePath, nodeRef *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node], err error) error {
		addr := path.Node(-1).ToNodeAddr

		// Node
		var treeID btrfsprim.ObjID
		var cliqueID btrfsprim.ObjID
		var nodeStr string
		if err != nil && (errors.Is(err, btrfstree.ErrNotANode) || errors.As(err, new(*btrfstree.IOError))) {
			treeID = 0
			func() {
				defer func() {
					if r := recover(); r != nil {
						panic(fmt.Errorf("path=%#v (%s): %v", path, path, r))
					}
				}()
				cliqueID = getCliqueID(cliques, path.Node(-1).FromTree)
			}()
			nodeStr = fmt.Sprintf(`n%d [shape=star color=red label="%v"]`, addr, addr)
		} else {
			treeID = nodeRef.Data.Head.Owner
			cliqueID = getCliqueID(cliques, treeID)
			var buf strings.Builder
			fmt.Fprintf(&buf, `n%d [shape=record label="%v\ngen=%v\nlvl=%v|{`,
				addr,
				nodeRef.Data.Head.Addr,
				nodeRef.Data.Head.Generation,
				nodeRef.Data.Head.Level)
			if nodeRef.Data.Head.NumItems == 0 {
				buf.WriteString("(no items)")
			} else {
				for i := uint32(0); i < nodeRef.Data.Head.NumItems; i++ {
					if i == 0 {
						fmt.Fprintf(&buf, "<p%[1]d>%[1]d", i)
					} else {
						fmt.Fprintf(&buf, "|<p%[1]d>%[1]d", i)
					}
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
		fmt.Fprintf(&edge, ` -> n%d [label="`, addr)
		if path.Node(-1).FromItemIdx >= 0 {
			fmt.Fprintf(&edge, "%d: key=(%d,%v,%d) gen=%v",
				path.Node(-1).FromItemIdx,
				path.Node(-1).ToKey.ObjectID,
				path.Node(-1).ToKey.ItemType,
				path.Node(-1).ToKey.Offset,
				path.Node(-1).ToNodeGeneration)
		}
		if err != nil {
			fmt.Fprintf(&edge, `\n\n%s" color=red]`, html.EscapeString(err.Error()))
		} else {
			edge.WriteString(`"]`)
		}
		if _, ok := edges[cliqueID]; !ok {
			edges[cliqueID] = make(containers.Set[string])
		}
		edges[cliqueID].Insert(edge.String())

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

	dlog.Info(ctx, "... done building")

	////////////////////////////////////////////////////////////////////////////////////////////

	dlog.Infof(ctx, "Writing graphviz output to %q...", dir)

	cliqueIDs := maps.SortedKeys(edges)

	if err := os.MkdirAll(dir, 0777); err != nil {
		return err
	}
	for _, cliqueID := range cliqueIDs {
		if err := func() (err error) {
			maybeSetErr := func(_err error) {
				if err == nil && _err != nil {
					err = _err
				}
			}
			fh, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d.dot", cliqueID)), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
			if err != nil {
				return err
			}
			defer func() {
				maybeSetErr(fh.Close())
			}()
			buf := bufio.NewWriter(fh)
			defer func() {
				maybeSetErr(buf.Flush())
			}()

			if _, err := fmt.Fprintf(buf, "digraph clique%d {\n", cliqueID); err != nil {
				return err
			}
			clique := cliques[cliqueID]
			for _, treeID := range maps.SortedKeys(*clique) {
				if _, err := fmt.Fprintf(buf, "  subgraph cluster_tree%d {\n", treeID); err != nil {
					return err
				}
				for _, node := range maps.SortedKeys(nodes[treeID]) {
					if _, err := fmt.Fprintf(buf, "    %s;\n", node); err != nil {
						return err
					}
				}
				if _, err := fmt.Fprintln(buf, "  }"); err != nil {
					return err
				}
			}
			for _, edge := range maps.SortedKeys(edges[cliqueID]) {
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

	dlog.Info(ctx, "... done writing")

	return nil
}
