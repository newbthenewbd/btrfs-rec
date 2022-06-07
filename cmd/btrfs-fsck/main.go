package main

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
)

func main() {
	if err := Main(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "%s: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

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
	fs := &btrfs.FS{
		Devices: []*btrfs.Device{
			{
				File: fh,
			},
		},
	}

	superblock, err := pass0(fs)
	if err != nil {
		return err
	}

	_ /*allNodes*/, err = pass1(fs, superblock)
	if err != nil {
		return err
	}

	fmt.Printf("\nPass 2: orphaned nodes\n") ///////////////////////////////////////////////////
	/*

		fmt.Printf("node@%d: physical_addr=0x%0X logical_addr=0x%0X generation=%d owner=%v level=%d\n",
			nodeRef.Addr,
			nodeRef.Addr, nodeRef.Data.Head.Addr,
			nodeRef.Data.Head.Generation, nodeRef.Data.Head.Owner, nodeRef.Data.Head.Level)
		srcPaddr := btrfs.QualifiedPhysicalAddr{
			Dev:  superblock.Data.DevItem.DevUUID,
			Addr: nodeRef.Addr,
		}
		resPaddrs, _ := fs.Resolve(nodeRef.Data.Head.Addr)
		if len(resPaddrs) == 0 {
			fmt.Printf("node@%d: logical_addr=0x%0X is not mapped\n",
				nodeRef.Addr, nodeRef.Data.Head.Addr)
		} else if _, ok := resPaddrs[srcPaddr]; !ok {
			fmt.Printf("node@%d: logical_addr=0x%0X maps to %v, not %v\n",
				nodeRef.Addr, nodeRef.Data.Head.Addr, resPaddrs, srcPaddr)
		}
	*/
	return nil
}

func walkFS(fs *btrfs.FS, cbs btrfs.WalkTreeHandler, errCb func(error)) {
	origItem := cbs.Item
	cbs.Item = func(key btrfs.Key, body btrfsitem.Item) error {
		if key.ItemType == btrfsitem.ROOT_ITEM_KEY {
			root, ok := body.(btrfsitem.Root)
			if !ok {
				errCb(fmt.Errorf("ROOT_ITEM_KEY is a %T, not a btrfsitem.Root", body))
			} else if err := fs.WalkTree(root.ByteNr, cbs); err != nil {
				errCb(err)
			}
		}
		if origItem != nil {
			return origItem(key, body)
		}
		return nil
	}

	superblock, err := fs.Superblock()
	if err != nil {
		errCb(err)
		return
	}

	if err := fs.WalkTree(superblock.Data.RootTree, cbs); err != nil {
		errCb(err)
	}
	if err := fs.WalkTree(superblock.Data.ChunkTree, cbs); err != nil {
		errCb(err)
	}
	if err := fs.WalkTree(superblock.Data.LogTree, cbs); err != nil {
		errCb(err)
	}
	if err := fs.WalkTree(superblock.Data.BlockGroupRoot, cbs); err != nil {
		errCb(err)
	}
}
