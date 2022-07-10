// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsrepair

import (
	"errors"
	"fmt"
	"io"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

func ClearBadNodes(out, errout io.Writer, fs *btrfs.FS) error {
	var uuidsInited bool
	var metadataUUID, chunkTreeUUID btrfs.UUID

	var treeName string
	var treeID btrfs.ObjID
	btrfsutil.WalkAllTrees(fs, btrfsutil.WalkAllTreesHandler{
		PreTree: func(name string, id btrfs.ObjID) {
			treeName = name
			treeID = id
		},
		Err: func(err error) {
			fmt.Fprintf(errout, "error: %v\n", err)
		},
		UnsafeNodes: true,
		TreeWalkHandler: btrfs.TreeWalkHandler{
			Node: func(path btrfs.TreePath, node *util.Ref[btrfsvol.LogicalAddr, btrfs.Node], err error) error {
				if err == nil {
					if !uuidsInited {
						metadataUUID = node.Data.Head.MetadataUUID
						chunkTreeUUID = node.Data.Head.ChunkTreeUUID
						uuidsInited = true
					}
					return nil
				}
				if !errors.Is(err, btrfs.ErrNotANode) {
					err = btrfsutil.WalkErr{
						TreeName: treeName,
						Path:     path,
						Err:      err,
					}
					fmt.Fprintf(errout, "error: %v\n", err)
					return nil
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
						Generation:    0,
						Owner:         treeID,
						NumItems:      0,
						Level:         path[len(path)-1].NodeLevel,
					},
				}
				node.Data.Head.Checksum, err = node.Data.CalculateChecksum()
				if err != nil {
					return btrfsutil.WalkErr{
						TreeName: treeName,
						Path:     path,
						Err:      err,
					}
				}
				if err := node.Write(); err != nil {
					return err
				}

				fmt.Fprintf(out, "fixed node@%v (err was %v)\n", node.Addr, origErr)
				return nil
			},
		},
	})
	return nil
}
