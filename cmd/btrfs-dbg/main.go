package main

import (
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"

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

	superblocks, err := fs.Devices[0].Superblocks()
	if err != nil {
		return err
	}

	spew := spew.NewDefaultConfig()
	spew.DisablePointerAddresses = true

	sum, err := superblocks[0].Data.CalculateChecksum()
	if err != nil {
		return err
	}
	fmt.Printf("superblock checksum: %x\n", sum)
	spew.Dump(superblocks[0].Data)

	syschunks, err := superblocks[0].Data.ParseSysChunkArray()
	if err != nil {
		return err
	}
	spew.Dump(syschunks)

	if err := btrfsmisc.ScanForNodes(fs.Devices[0], superblocks[0].Data, func(nodeRef *util.Ref[btrfs.PhysicalAddr, btrfs.Node], err error) {
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("node@%d: physical_addr=0x%0X logical_addr=0x%0X generation=%d owner=%v level=%d\n",
				nodeRef.Addr,
				nodeRef.Addr, nodeRef.Data.Head.Addr,
				nodeRef.Data.Head.Generation, nodeRef.Data.Head.Owner, nodeRef.Data.Head.Level)
		}
	}); err != nil {
		return err
	}

	return nil
}
