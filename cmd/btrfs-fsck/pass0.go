package main

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func pass0(filenames ...string) (*btrfs.FS, *util.Ref[btrfsvol.PhysicalAddr, btrfs.Superblock], error) {
	fmt.Printf("\nPass 0: init and superblocks...\n")

	fs := new(btrfs.FS)
	for _, filename := range filenames {
		fmt.Printf("Pass 0: ... adding device %q...\n", filename)

		fh, err := os.OpenFile(filename, os.O_RDWR, 0)
		if err != nil {
			_ = fs.Close()
			return nil, nil, fmt.Errorf("device %q: %w", filename, err)
		}

		if err := fs.AddDevice(&btrfs.Device{File: fh}); err != nil {
			fmt.Printf("Pass 0: ... add device %q: error: %v\n", filename, err)
		}
	}

	sb, err := fs.Superblock()
	if err != nil {
		_ = fs.Close()
		return nil, nil, err
	}

	return fs, sb, nil
}
