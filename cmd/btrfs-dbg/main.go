// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsmisc"
	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

func main() {
	if err := Main(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "%v: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

func Main(imgfilename string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	fs, err := btrfsmisc.Open(os.O_RDONLY, imgfilename)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fs.Close())
	}()

	superblocks, err := fs.Superblocks()
	if err != nil {
		return err
	}

	spew := spew.NewDefaultConfig()
	spew.DisablePointerAddresses = true

	sum, err := superblocks[0].Data.CalculateChecksum()
	if err != nil {
		return err
	}
	fmt.Printf("superblock checksum: %v\n", sum)
	spew.Dump(superblocks[0].Data)

	syschunks, err := superblocks[0].Data.ParseSysChunkArray()
	if err != nil {
		return err
	}
	spew.Dump(syschunks)

	for _, dev := range fs.LV.PhysicalVolumes() {
		if err := btrfsmisc.ScanForNodes(dev, superblocks[0].Data, func(nodeRef *util.Ref[btrfsvol.PhysicalAddr, btrfs.Node], err error) {
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Printf("node@%v: physical_addr=%v logical_addr=%v generation=%v owner=%v level=%v\n",
					nodeRef.Addr,
					nodeRef.Addr, nodeRef.Data.Head.Addr,
					nodeRef.Data.Head.Generation, nodeRef.Data.Head.Owner, nodeRef.Data.Head.Level)
			}
		}, nil); err != nil {
			return err
		}
	}

	return nil
}
