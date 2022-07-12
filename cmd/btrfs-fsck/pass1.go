// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"context"
	"fmt"
	"os"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

func pass1(ctx context.Context, fs *btrfs.FS, superblock *util.Ref[btrfsvol.PhysicalAddr, btrfs.Superblock]) (map[btrfsvol.LogicalAddr]struct{}, error) {
	fmt.Printf("\nPass 1: chunk mappings...\n")

	fmt.Printf("Pass 1: ... walking fs\n")
	visitedNodes := make(map[btrfsvol.LogicalAddr]struct{})
	btrfsutil.WalkAllTrees(fs, btrfsutil.WalkAllTreesHandler{
		TreeWalkHandler: btrfs.TreeWalkHandler{
			Node: func(path btrfs.TreePath, node *util.Ref[btrfsvol.LogicalAddr, btrfs.Node], err error) error {
				if err != nil {
					err = fmt.Errorf("%v: %w", path, err)
					fmt.Printf("Pass 1: ... walk fs: error: %v\n", err)
				}
				if node != nil {
					visitedNodes[node.Addr] = struct{}{}
				}
				return err
			},
		},
		Err: func(err error) {
			fmt.Printf("Pass 1: ... walk fs: error: %v\n", err)
		},
	})

	fsFoundNodes := make(map[btrfsvol.LogicalAddr]struct{})
	for _, dev := range fs.LV.PhysicalVolumes() {
		fmt.Printf("Pass 1: ... dev[%q] scanning for nodes...\n", dev.Name())
		devResult, err := btrfsinspect.ScanOneDev(ctx, dev, superblock.Data)
		if err != nil {
			return nil, err
		}

		fmt.Printf("Pass 1: ... dev[%q] re-inserting lost+found mappings\n", dev.Name())
		devResult.AddToLV(ctx, fs, dev)

		// merge those results in to the total-fs results
		for laddr := range devResult.FoundNodes {
			fsFoundNodes[laddr] = struct{}{}
		}
	}

	fmt.Printf("Pass 1: ... logical address space:\n")
	btrfsinspect.PrintLogicalSpace(os.Stdout, fs)
	fmt.Printf("Pass 1: ... physical address space:\n")
	btrfsinspect.PrintPhysicalSpace(os.Stdout, fs)

	fmt.Printf("Pass 1: ... writing re-constructed chunks\n")
	pass1WriteReconstructedChunks(fs)

	return fsFoundNodes, nil
}

func pass1WriteReconstructedChunks(fs *btrfs.FS) {
	sbRef, _ := fs.Superblock()
	superblock := sbRef.Data

	// FIXME(lukeshu): OK, so this just assumes that all the
	// reconstructed stripes fit in one node, and that we can just
	// store that node at the root node of the chunk tree.  This
	// isn't true in general, but it's true of my particular
	// filesystem.
	reconstructedNode := &util.Ref[btrfsvol.LogicalAddr, btrfs.Node]{
		File: fs,
		Addr: superblock.ChunkTree,
		Data: btrfs.Node{
			Size: superblock.NodeSize,
			Head: btrfs.NodeHeader{
				MetadataUUID: superblock.EffectiveMetadataUUID(),
				Addr:         superblock.ChunkTree,
				Flags:        btrfs.NodeWritten,
				//BackrefRef: ???,
				//ChunkTreeUUID: ???,
				Generation: superblock.ChunkRootGeneration,
				Owner:      btrfs.CHUNK_TREE_OBJECTID,
				Level:      0,
			},
		},
	}

	for _, dev := range fs.LV.PhysicalVolumes() {
		superblock, _ := dev.Superblock()
		reconstructedNode.Data.BodyLeaf = append(reconstructedNode.Data.BodyLeaf, btrfs.Item{
			Head: btrfs.ItemHeader{
				Key: btrfs.Key{
					ObjectID: btrfs.DEV_ITEMS_OBJECTID,
					ItemType: btrfsitem.DEV_ITEM_KEY,
					Offset:   uint64(superblock.Data.DevItem.DevID),
				},
			},
			Body: superblock.Data.DevItem,
		})
	}

	for _, mapping := range fs.LV.Mappings() {
		chunkIdx := len(reconstructedNode.Data.BodyLeaf) - 1
		if len(reconstructedNode.Data.BodyLeaf) == 0 || reconstructedNode.Data.BodyLeaf[chunkIdx].Head.Key.Offset != uint64(mapping.LAddr) {
			reconstructedNode.Data.BodyLeaf = append(reconstructedNode.Data.BodyLeaf, btrfs.Item{
				Head: btrfs.ItemHeader{
					Key: btrfs.Key{
						ObjectID: btrfs.FIRST_CHUNK_TREE_OBJECTID,
						ItemType: btrfsitem.CHUNK_ITEM_KEY,
						Offset:   uint64(mapping.LAddr),
					},
				},
				Body: btrfsitem.Chunk{
					Head: btrfsitem.ChunkHeader{
						Size:           mapping.Size,
						Owner:          btrfs.EXTENT_TREE_OBJECTID,
						StripeLen:      65536, // ???
						Type:           *mapping.Flags,
						IOOptimalAlign: superblock.DevItem.IOOptimalAlign,
						IOOptimalWidth: superblock.DevItem.IOOptimalWidth,
						IOMinSize:      superblock.DevItem.IOMinSize,
						SubStripes:     1,
					},
				},
			})
			chunkIdx++
		}
		dev := fs.LV.PhysicalVolumes()[mapping.PAddr.Dev]
		devSB, _ := dev.Superblock()
		chunkBody := reconstructedNode.Data.BodyLeaf[chunkIdx].Body.(btrfsitem.Chunk)
		chunkBody.Stripes = append(chunkBody.Stripes, btrfsitem.ChunkStripe{
			DeviceID:   mapping.PAddr.Dev,
			Offset:     mapping.PAddr.Addr,
			DeviceUUID: devSB.Data.DevItem.DevUUID,
		})
		reconstructedNode.Data.BodyLeaf[chunkIdx].Body = chunkBody
	}

	var err error
	reconstructedNode.Data.Head.Checksum, err = reconstructedNode.Data.CalculateChecksum()
	if err != nil {
		fmt.Printf("Pass 1: ... new node checksum: error: %v\n", err)
	}

	if err := reconstructedNode.Write(); err != nil {
		fmt.Printf("Pass 1: ... write new node: error: %v\n", err)
	}

	if err := fs.ReInit(); err != nil {
		fmt.Printf("Pass 1: ... re-init mappings: %v\n", err)
	}

	sbs, _ := fs.Superblocks()
	for i, sb := range sbs {
		sb.Data.ChunkLevel = reconstructedNode.Data.Head.Level
		sb.Data.Checksum, err = sb.Data.CalculateChecksum()
		if err != nil {
			fmt.Printf("Pass 1: ... calculate superblock %d checksum: %v\n", i, err)
		}
		if err := sb.Write(); err != nil {
			fmt.Printf("Pass 1: ... write superblock %d: %v\n", i, err)
		}
	}
}
