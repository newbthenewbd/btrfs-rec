// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

/*
import (
	"fmt"
	"reflect"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

type RebuiltNode struct {
	Errs           containers.Set[string]
	MinKey, MaxKey btrfsprim.Key
	InTrees        containers.Set[btrfsprim.ObjID]
	btrfstree.Node
}

func (a RebuiltNode) Compat(b RebuiltNode) bool {
	a.Node.Head.Generation = b.Node.Head.Generation
	return reflect.DeepEqual(a.Node, b.Node)
}

func (a RebuiltNode) Merge(b RebuiltNode) (RebuiltNode, error) {
	if !a.Compat(b) {
		switch {
		case a.Node.Head.Generation > b.Node.Head.Generation:
			return a, nil
		case a.Node.Head.Generation < b.Node.Head.Generation:
			return b, nil
		default:
			return a, fmt.Errorf("mismatch: %v != %v", a, b)
		}
	}

	// take the broadest region
	if a.MinKey.Cmp(b.MinKey) > 0 { // if a.MinKey > b.MinKey {
		a.MinKey = b.MinKey // take the min of the two
	}
	if a.MaxKey.Cmp(b.MaxKey) < 0 { // if a.MaxKey < b.MaxKey {
		a.MaxKey = b.MaxKey // take the min of the two
	}

	// take the highest generation
	a.Node.Head.Generation = slices.Max(a.Node.Head.Generation, b.Node.Head.Generation)

	// take the union
	a.InTrees.InsertFrom(b.InTrees)
	a.Errs.InsertFrom(b.Errs)

	return a, nil
}

func reInitBrokenNodes(ctx context.Context, fs _FS, badNodes []badNode) (map[btrfsvol.LogicalAddr]*RebuiltNode, error) {
	dlog.Info(ctx, "Re-initializing bad nodes...")

	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}
	chunkTreeUUID, ok := getChunkTreeUUID(ctx, fs)
	if !ok {
		return nil, fmt.Errorf("could not look up chunk tree UUID")
	}

	sort.Slice(badNodes, func(i, j int) bool {
		iGen := badNodes[i].Path.Node(-1).ToNodeGeneration
		jGen := badNodes[j].Path.Node(-1).ToNodeGeneration
		switch {
		case iGen < jGen:
			return true
		case iGen > jGen:
			return false
		default:
			iAddr := badNodes[i].Path.Node(-1).ToNodeAddr
			jAddr := badNodes[j].Path.Node(-1).ToNodeAddr
			return iAddr < jAddr
		}
	})

	lastPct := -1
	progress := func(done int) {
		pct := int(100 * float64(done) / float64(len(badNodes)))
		if pct != lastPct || done == len(badNodes) {
			dlog.Infof(ctx, "... %v%% (%v/%v)",
				pct, done, len(badNodes))
			lastPct = pct
		}
	}

	rebuiltNodes := make(map[btrfsvol.LogicalAddr]*RebuiltNode)
	for i, badNode := range badNodes {
		progress(i)
		path := badNode.Path

		min, max := spanOfTreePath(fs, path)
		node := RebuiltNode{
			Errs: containers.NewSet[string](
				badNode.Err,
			),
			MinKey: min,
			MaxKey: max,
			InTrees: containers.NewSet[btrfsprim.ObjID](
				path.Node(-1).FromTree,
			),
			Node: btrfstree.Node{
				Size:         sb.NodeSize,
				ChecksumType: sb.ChecksumType,
				Head: btrfstree.NodeHeader{
					MetadataUUID:  sb.EffectiveMetadataUUID(),
					Addr:          path.Node(-1).ToNodeAddr,
					ChunkTreeUUID: chunkTreeUUID,
					//Owner:      TBD, // see RebuiltNode.InTrees
					Generation: path.Node(-1).ToNodeGeneration,
					Level:      path.Node(-1).ToNodeLevel,
				},
			},
		}
		if other, ok := rebuiltNodes[path.Node(-1).ToNodeAddr]; ok {
			*other, err = other.Merge(node)
			if err != nil {
				dlog.Errorf(ctx, "... %v", err)
			}
		} else {
			rebuiltNodes[path.Node(-1).ToNodeAddr] = &node
		}
	}
	progress(len(badNodes))

	dlog.Infof(ctx, "... initialized %d nodes", len(rebuiltNodes))
	return rebuiltNodes, nil
}
*/
