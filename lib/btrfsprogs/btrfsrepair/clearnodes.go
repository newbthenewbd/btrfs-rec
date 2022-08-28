// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsrepair

import (
	"context"
	"errors"
	"fmt"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

func ClearBadNodes(ctx context.Context, fs *btrfs.FS) error {
	var uuidsInited bool
	var metadataUUID, chunkTreeUUID btrfs.UUID

	btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
		Err: func(err *btrfsutil.WalkError) {
			dlog.Error(ctx, err)
		},
		TreeWalkHandler: btrfs.TreeWalkHandler{
			Node: func(path btrfs.TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node]) error {
				if !uuidsInited {
					metadataUUID = node.Data.Head.MetadataUUID
					chunkTreeUUID = node.Data.Head.ChunkTreeUUID
					uuidsInited = true
				}
				return nil
			},
			BadNode: func(path btrfs.TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node], err error) error {
				if !errors.Is(err, btrfs.ErrNotANode) {
					return err
				}
				origErr := err
				if !uuidsInited {
					// TODO(lukeshu): Is there a better way to get the chunk
					// tree UUID?
					return fmt.Errorf("cannot repair node@%v: not (yet?) sure what the chunk tree UUID is", node.Addr)
				}
				node.Data = btrfs.Node{
					Size:         node.Data.Size,
					ChecksumType: node.Data.ChecksumType,
					Head: btrfs.NodeHeader{
						//Checksum:   filled below,
						MetadataUUID:  metadataUUID,
						Addr:          node.Addr,
						Flags:         btrfs.NodeWritten,
						BackrefRev:    btrfs.MixedBackrefRev,
						ChunkTreeUUID: chunkTreeUUID,
						Generation:    path.Node(-1).FromGeneration,
						Owner:         path.Node(-1).FromTree,
						NumItems:      0,
						Level:         path.Node(-1).ToNodeLevel,
					},
				}
				node.Data.Head.Checksum, err = node.Data.CalculateChecksum()
				if err != nil {
					return err
				}
				if err := node.Write(); err != nil {
					return err
				}

				dlog.Infof(ctx, "fixed node@%v (err was %v)\n", node.Addr, origErr)
				return nil
			},
		},
	})
	return nil
}
