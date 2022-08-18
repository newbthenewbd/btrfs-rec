// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"

	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:  "rebuild-nodes NODESCAN.json",
			Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			dlog.Infof(ctx, "Reading %q...", args[0])
			nodeScanResults, err := readNodeScanResults(args[0])
			if err != nil {
				return err
			}
			runtime.GC()
			dlog.Infof(ctx, "... done reading %q", args[0])

			dlog.Info(ctx, "Identifying lost+found nodes...")
			foundRoots, err := lostAndFoundNodes(ctx, fs, nodeScanResults)
			if err != nil {
				return err
			}
			dlog.Infof(ctx, "... identified %d lost+found nodes", len(foundRoots))

			dlog.Info(ctx, "Initializing nodes to re-build...")
			rebuiltNodes, err := reInitBrokenNodes(ctx, fs, nodeScanResults, foundRoots)
			if err != nil {
				return err
			}
			dlog.Infof(ctx, "Initialized %d nodes", len(rebuiltNodes))

			dlog.Info(ctx, "Attaching lost+found nodes to rebuilt nodes...")
			if err := reAttachNodes(ctx, fs, foundRoots, rebuiltNodes); err != nil {
				return err
			}
			dlog.Info(ctx, "... done attaching")

			dlog.Info(ctx, "Writing re-built nodes to stdout...")
			encoder := json.NewEncoder(os.Stdout)
			encoder.SetIndent("", "    ")
			if err := encoder.Encode(rebuiltNodes); err != nil {
				return err
			}
			dlog.Info(ctx, "... done writing")

			return nil
		},
	})
}

type NodeScanResults = map[btrfsvol.DeviceID]btrfsinspect.ScanOneDeviceResult

func readNodeScanResults(filename string) (NodeScanResults, error) {
	scanResultsBytes, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var scanResults NodeScanResults
	if err := json.Unmarshal(scanResultsBytes, &scanResults); err != nil {
		return nil, err
	}

	return scanResults, nil
}

var maxKey = btrfs.Key{
	ObjectID: math.MaxUint64,
	ItemType: math.MaxUint8,
	Offset:   math.MaxUint64,
}

func keyMm(key btrfs.Key) btrfs.Key {
	switch {
	case key.Offset > 0:
		key.Offset--
	case key.ItemType > 0:
		key.ItemType--
	case key.ObjectID > 0:
		key.ObjectID--
	}
	return key
}

func spanOfTreePath(fs *btrfs.FS, path btrfs.TreePath) (btrfs.Key, btrfs.Key) {
	// tree root error
	if len(path.Nodes) == 0 {
		return btrfs.Key{}, maxKey
	}

	// item error
	if path.Node(-1).NodeAddr == 0 {
		// If we got an item error, then the node is readable
		node, _ := fs.ReadNode(path.Node(-2).NodeAddr)
		key := node.Data.BodyLeaf[path.Node(-1).ItemIdx].Key
		return key, key
	}

	// node error
	//
	// assume that path.Node(-1).NodeAddr is not readable, but that path.Node(-2).NodeAddr is.
	if len(path.Nodes) == 1 {
		return btrfs.Key{}, maxKey
	}
	parentNode, _ := fs.ReadNode(path.Node(-2).NodeAddr)
	low := parentNode.Data.BodyInternal[path.Node(-1).ItemIdx].Key
	var high btrfs.Key
	if path.Node(-1).ItemIdx+1 < len(parentNode.Data.BodyInternal) {
		high = keyMm(parentNode.Data.BodyInternal[path.Node(-1).ItemIdx+1].Key)
	} else {
		parentPath := path.DeepCopy()
		parentPath.Nodes = parentPath.Nodes[:len(parentPath.Nodes)-1]
		_, high = spanOfTreePath(fs, parentPath)
	}
	return low, high
}

func walkFromNode(ctx context.Context, fs *btrfs.FS, nodeAddr btrfsvol.LogicalAddr, errHandle func(*btrfs.TreeError), cbs btrfs.TreeWalkHandler) {
	nodeRef, _ := fs.ReadNode(nodeAddr)
	if nodeRef == nil {
		return
	}
	treeInfo := btrfs.TreeRoot{
		TreeID:     nodeRef.Data.Head.Owner,
		RootNode:   nodeAddr,
		Level:      nodeRef.Data.Head.Level,
		Generation: nodeRef.Data.Head.Generation,
	}
	fs.RawTreeWalk(ctx, treeInfo, errHandle, cbs)
}

func countNodes(nodeScanResults NodeScanResults) int {
	var cnt int
	for _, devResults := range nodeScanResults {
		cnt += len(devResults.FoundNodes)
	}
	return cnt
}

func lostAndFoundNodes(ctx context.Context, fs *btrfs.FS, nodeScanResults NodeScanResults) (map[btrfsvol.LogicalAddr]struct{}, error) {
	lastPct := -1
	total := countNodes(nodeScanResults)
	progress := func(done int) {
		pct := int(100 * float64(done) / float64(total))
		if pct != lastPct || done == total {
			dlog.Infof(ctx, "... %v%% (%v/%v)",
				pct, done, total)
			lastPct = pct
		}
	}
	var done int

	attachedNodes := make(map[btrfsvol.LogicalAddr]struct{})
	btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
		TreeWalkHandler: btrfs.TreeWalkHandler{
			Node: func(path btrfs.TreePath, _ *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node]) error {
				attachedNodes[path.Node(-1).NodeAddr] = struct{}{}
				done++
				progress(done)
				return nil
			},
		},
		Err: func(err *btrfsutil.WalkError) {
			// do nothing
		},
	})

	orphanedNodes := make(map[btrfsvol.LogicalAddr]int)
	for _, devResults := range nodeScanResults {
		for laddr := range devResults.FoundNodes {
			if _, attached := attachedNodes[laddr]; !attached {
				orphanedNodes[laddr] = 0
			}
		}
	}
	dlog.Infof(ctx,
		"... (finished processing %v attached nodes, proceeding to process %v lost nodes, for a total of %v)",
		done, len(orphanedNodes), done+len(orphanedNodes))

	// 'orphanedRoots' is a subset of 'orphanedNodes'; start with
	// it as the complete orphanedNodes, and then remove entries.
	orphanedRoots := make(map[btrfsvol.LogicalAddr]struct{}, len(orphanedNodes))
	for node := range orphanedNodes {
		orphanedRoots[node] = struct{}{}
	}
	for potentialRoot := range orphanedRoots {
		done++
		progress(done)
		if orphanedNodes[potentialRoot] > 1 {
			continue
		}
		walkCtx, cancel := context.WithCancel(ctx)
		walkFromNode(walkCtx, fs, potentialRoot,
			func(err *btrfs.TreeError) {
				// do nothing
			},
			btrfs.TreeWalkHandler{
				PreNode: func(path btrfs.TreePath) error {
					nodeAddr := path.Node(-1).NodeAddr
					if nodeAddr != potentialRoot {
						delete(orphanedRoots, nodeAddr)
					}
					visitCnt := orphanedNodes[nodeAddr] + 1
					orphanedNodes[nodeAddr] = visitCnt
					if visitCnt > 1 {
						cancel()
					}
					return nil
				},
			},
		)
	}

	return orphanedRoots, nil
}

func getChunkTreeUUID(ctx context.Context, fs *btrfs.FS) (btrfs.UUID, bool) {
	ctx, cancel := context.WithCancel(ctx)
	var ret btrfs.UUID
	var retOK bool
	btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
		TreeWalkHandler: btrfs.TreeWalkHandler{
			Node: func(path btrfs.TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node]) error {
				ret = node.Data.Head.ChunkTreeUUID
				retOK = true
				cancel()
				return nil
			},
		},
		Err: func(err *btrfsutil.WalkError) {
			// do nothing
		},
	})
	return ret, retOK
}

type rebuiltNode struct {
	Err            error
	MinKey, MaxKey btrfs.Key
	btrfs.Node
}

func reInitBrokenNodes(ctx context.Context, fs *btrfs.FS, nodeScanResults NodeScanResults, foundRoots map[btrfsvol.LogicalAddr]struct{}) (map[btrfsvol.LogicalAddr]*rebuiltNode, error) {
	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}

	chunkTreeUUID, ok := getChunkTreeUUID(ctx, fs)
	if !ok {
		return nil, fmt.Errorf("could not look up chunk tree UUID")
	}

	lastPct := -1
	total := countNodes(nodeScanResults)
	progress := func(done int) {
		pct := int(100 * float64(done) / float64(total))
		if pct != lastPct || done == total {
			dlog.Infof(ctx, "... %v%% (%v/%v)",
				pct, done, total)
			lastPct = pct
		}
	}
	var done int

	rebuiltNodes := make(map[btrfsvol.LogicalAddr]*rebuiltNode)
	walkHandler := btrfs.TreeWalkHandler{
		Node: func(_ btrfs.TreePath, _ *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node]) error {
			done++
			progress(done)
			return nil
		},
		BadNode: func(path btrfs.TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node], err error) error {
			min, max := spanOfTreePath(fs, path)
			rebuiltNodes[path.Node(-1).NodeAddr] = &rebuiltNode{
				Err:    err,
				MinKey: min,
				MaxKey: max,
				Node: btrfs.Node{
					Head: btrfs.NodeHeader{
						MetadataUUID:  sb.EffectiveMetadataUUID(),
						Addr:          path.Node(-1).NodeAddr,
						ChunkTreeUUID: chunkTreeUUID,
						Owner:         path.TreeID,
						Level:         path.Node(-1).NodeLevel,
					},
				},
			}
			return err
		},
	}

	btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
		Err: func(err *btrfsutil.WalkError) {
			// do nothing
		},
		TreeWalkHandler: walkHandler,
	})
	for foundRoot := range foundRoots {
		walkFromNode(ctx, fs, foundRoot,
			func(err *btrfs.TreeError) {
				// do nothing
			},
			walkHandler)
	}

	return rebuiltNodes, nil
}

func reAttachNodes(ctx context.Context, fs *btrfs.FS, foundRoots map[btrfsvol.LogicalAddr]struct{}, rebuiltNodes map[btrfsvol.LogicalAddr]*rebuiltNode) error {
	// Index 'rebuiltNodes' for fast lookups.
	gaps := make(map[btrfs.ObjID]map[uint8][]*rebuiltNode)
	maxLevel := make(map[btrfs.ObjID]uint8)
	for _, node := range rebuiltNodes {
		maxLevel[node.Head.Owner] = slices.Max(maxLevel[node.Head.Owner], node.Head.Level)

		if gaps[node.Head.Owner] == nil {
			gaps[node.Head.Owner] = make(map[uint8][]*rebuiltNode)
		}
		gaps[node.Head.Owner][node.Head.Level] = append(gaps[node.Head.Owner][node.Head.Level], node)
	}
	for _, byTreeID := range gaps {
		for _, slice := range byTreeID {
			sort.Slice(slice, func(i, j int) bool {
				return slice[i].MinKey.Cmp(slice[j].MinKey) < 0
			})
		}
	}

	// Attach foundRoots to the gaps.
	for foundLAddr := range foundRoots {
		foundRef, err := fs.ReadNode(foundLAddr)
		if foundRef == nil {
			return err
		}
		foundMinKey, ok := foundRef.Data.MinItem()
		if !ok {
			continue
		}
		foundMaxKey, ok := foundRef.Data.MaxItem()
		if !ok {
			continue
		}
		treeGaps := gaps[foundRef.Data.Head.Owner]
		var attached bool
		for level := foundRef.Data.Head.Level + 1; treeGaps != nil && level <= maxLevel[foundRef.Data.Head.Owner] && !attached; level++ {
			parentGen, ok := treeGaps[level]
			if !ok {
				continue
			}
			parentIdx, ok := slices.Search(parentGen, func(parent *rebuiltNode) int {
				switch {
				case foundMinKey.Cmp(parent.MinKey) < 0:
					// 'parent' is too far right
					return -1
				case foundMaxKey.Cmp(parent.MaxKey) > 0:
					// 'parent' is too far left
					return 1
				default:
					// just right
					return 0
				}
			})
			if !ok {
				continue
			}
			parent := parentGen[parentIdx]
			parent.BodyInternal = append(parent.BodyInternal, btrfs.KeyPointer{
				Key:        foundMinKey,
				BlockPtr:   foundLAddr,
				Generation: foundRef.Data.Head.Generation,
			})
			attached = true
		}
		if !attached {
			dlog.Errorf(ctx, "could not find a broken node to attach node to reattach node@%v to",
				foundRef.Addr)
		}
	}

	return nil
}
