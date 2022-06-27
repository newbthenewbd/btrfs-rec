package main

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfsmisc"
)

func main() {
	if err := Main(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "%v: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

const version = "5.18.1"

func Main(imgfilename string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	fh, err := os.Open(imgfilename)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fh.Close())
	}()

	fs := new(btrfs.FS)
	if err := fs.AddDevice(&btrfs.Device{File: fh}); err != nil {
		fmt.Printf("(error) %v\n", err)
	}

	superblock, err := fs.Superblock()
	if err != nil {
		return err
	}

	fmt.Printf("btrfs-progs v%v\n", version)
	if superblock.Data.RootTree != 0 {
		fmt.Printf("root tree\n")
		if err := btrfsmisc.PrintTree(fs, superblock.Data.RootTree); err != nil {
			return err
		}
	}
	if superblock.Data.ChunkTree != 0 {
		fmt.Printf("chunk tree\n")
		if err := btrfsmisc.PrintTree(fs, superblock.Data.ChunkTree); err != nil {
			return err
		}
	}
	if superblock.Data.LogTree != 0 {
		fmt.Printf("log root tree\n")
		if err := btrfsmisc.PrintTree(fs, superblock.Data.LogTree); err != nil {
			return err
		}
	}
	if superblock.Data.BlockGroupRoot != 0 {
		fmt.Printf("block group tree\n")
		if err := btrfsmisc.PrintTree(fs, superblock.Data.BlockGroupRoot); err != nil {
			return err
		}
	}
	if err := fs.WalkTree(superblock.Data.RootTree, btrfs.WalkTreeHandler{
		Item: func(_ btrfs.WalkTreePath, item btrfs.Item) error {
			if item.Head.Key.ItemType != btrfsitem.ROOT_ITEM_KEY {
				return nil
			}
			treeName, ok := map[btrfs.ObjID]string{
				btrfs.ROOT_TREE_OBJECTID:        "root",
				btrfs.EXTENT_TREE_OBJECTID:      "extent",
				btrfs.CHUNK_TREE_OBJECTID:       "chunk",
				btrfs.DEV_TREE_OBJECTID:         "device",
				btrfs.FS_TREE_OBJECTID:          "fs",
				btrfs.ROOT_TREE_DIR_OBJECTID:    "directory",
				btrfs.CSUM_TREE_OBJECTID:        "checksum",
				btrfs.ORPHAN_OBJECTID:           "orphan",
				btrfs.TREE_LOG_OBJECTID:         "log",
				btrfs.TREE_LOG_FIXUP_OBJECTID:   "log fixup",
				btrfs.TREE_RELOC_OBJECTID:       "reloc",
				btrfs.DATA_RELOC_TREE_OBJECTID:  "data reloc",
				btrfs.EXTENT_CSUM_OBJECTID:      "extent checksum",
				btrfs.QUOTA_TREE_OBJECTID:       "quota",
				btrfs.UUID_TREE_OBJECTID:        "uuid",
				btrfs.FREE_SPACE_TREE_OBJECTID:  "free space",
				btrfs.MULTIPLE_OBJECTIDS:        "multiple",
				btrfs.BLOCK_GROUP_TREE_OBJECTID: "block group",
			}[item.Head.Key.ObjectID]
			if !ok {
				treeName = "file"
			}
			fmt.Printf("%v tree %v \n", treeName, btrfsmisc.FmtKey(item.Head.Key))
			return btrfsmisc.PrintTree(fs, item.Body.(btrfsitem.Root).ByteNr)
		},
	}); err != nil {
		return err
	}
	fmt.Printf("total bytes %v\n", superblock.Data.TotalBytes)
	fmt.Printf("bytes used %v\n", superblock.Data.BytesUsed)
	fmt.Printf("uuid %v\n", superblock.Data.FSUUID)

	return nil
}
