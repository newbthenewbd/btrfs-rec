package main

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func pass0(imgfiles ...*os.File) (*btrfs.FS, *util.Ref[btrfs.PhysicalAddr, btrfs.Superblock], error) {
	fmt.Printf("\nPass 0: init and superblocks...\n")

	fs := new(btrfs.FS)
	for _, imgfile := range imgfiles {
		fmt.Printf("Pass 0: ... adding device %q...\n", imgfile.Name())
		if err := fs.AddDevice(&btrfs.Device{File: imgfile}); err != nil {
			fmt.Printf("Pass 0: ... add device %q: error: %v\n", imgfile.Name(), err)
		}
	}

	sb, err := fs.Superblock()
	if err != nil {
		return nil, nil, err
	}

	return fs, sb, nil
}
