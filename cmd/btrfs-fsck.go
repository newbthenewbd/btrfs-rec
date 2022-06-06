package main

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfsmisc"
	"lukeshu.com/btrfs-tools/pkg/util"
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

	superblock, err := fs.Superblock()
	if err != nil {
		return fmt.Errorf("superblock: %w", err)
	}

	if err := fs.Init(); err != nil {
		fmt.Printf("init chunk tree: error: %v\n", err)
	}

	foundNodes := make(map[btrfs.LogicalAddr]struct{})

	if err := btrfsmisc.ScanForNodes(fs.Devices[0], superblock.Data, func(nodeRef *util.Ref[btrfs.PhysicalAddr, btrfs.Node], err error) {
		if err != nil {
			fmt.Println(err)
			return
		}
		foundNodes[nodeRef.Data.Head.Addr] = struct{}{}
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
	}); err != nil {
		return err
	}

	return nil
}
