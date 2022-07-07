package main

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/btrfsmisc"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func main() {
	if err := Main(os.Args[1:]...); err != nil {
		fmt.Fprintf(os.Stderr, "%v: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

func Main(imgfilenames ...string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	fs, err := btrfsmisc.Open(os.O_RDWR, imgfilenames...)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fs.Close())
	}()

	var treeName string
	var treeID btrfs.ObjID
	btrfsmisc.WalkFS(fs, btrfsmisc.WalkFSHandler{
		PreTree: func(name string, id btrfs.ObjID, _ btrfsvol.LogicalAddr) {
			treeName = name
			treeID = id
		},
		Err: func(err error) {
			fmt.Printf("error: %v\n", err)
		},
		UnsafeNodes: true,
		TreeWalkHandler: btrfs.TreeWalkHandler{
			Node: func(path btrfs.TreePath, node *util.Ref[btrfsvol.LogicalAddr, btrfs.Node], err error) error {
				if node == nil || err == nil {
					return nil
				}
				origErr := err
				if len(path) < 2 {
					sb, err := fs.Superblock()
					if err != nil {
						return err
					}
					chunkRoot, err := fs.TreeLookup(sb.Data.RootTree, btrfs.Key{
						ObjectID: btrfs.CHUNK_TREE_OBJECTID,
						ItemType: btrfsitem.ROOT_ITEM_KEY,
						Offset:   0,
					})
					if err != nil {
						return err
					}
					chunkRootBody, ok := chunkRoot.Body.(btrfsitem.Root)
					if !ok {
						return fmt.Errorf("CHUNK_TREE_OBJECTID ROOT_ITEM has malformed body")
					}
					node.Data = btrfs.Node{
						Size:         node.Data.Size,
						ChecksumType: node.Data.ChecksumType,
						Head: btrfs.NodeHeader{
							//Checksum:   filled below,
							MetadataUUID:  sb.Data.EffectiveMetadataUUID(),
							Addr:          node.Addr,
							Flags:         btrfs.NodeWritten,
							BackrefRev:    btrfs.MixedBackrefRev,
							ChunkTreeUUID: chunkRootBody.UUID,
							Generation:    0,
							Owner:         treeID,
							NumItems:      0,
							Level:         0,
						},
					}
				} else {
					parentNode, err := fs.ReadNode(path[len(path)-2].NodeAddr)
					if err != nil {
						return err
					}
					node.Data = btrfs.Node{
						Size:         node.Data.Size,
						ChecksumType: node.Data.ChecksumType,
						Head: btrfs.NodeHeader{
							//Checksum:   filled below,
							MetadataUUID:  parentNode.Data.Head.MetadataUUID,
							Addr:          node.Addr,
							Flags:         btrfs.NodeWritten,
							BackrefRev:    parentNode.Data.Head.BackrefRev,
							ChunkTreeUUID: parentNode.Data.Head.ChunkTreeUUID,
							Generation:    0,
							Owner:         parentNode.Data.Head.Owner,
							NumItems:      0,
							Level:         parentNode.Data.Head.Level - 1,
						},
					}
				}
				node.Data.Head.Checksum, err = node.Data.CalculateChecksum()
				if err != nil {
					return btrfsmisc.WalkErr{
						TreeName: treeName,
						Path:     path,
						Err:      err,
					}
				}
				if err := node.Write(); err != nil {
					return err
				}

				fmt.Printf("fixed node@%v (err was %v)\n", node.Addr, origErr)
				return nil
			},
		},
	})

	return nil
}
