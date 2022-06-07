package main

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func pass0(fs *btrfs.FS) (*util.Ref[btrfs.PhysicalAddr, btrfs.Superblock], error) {
	fmt.Printf("\nPass 0: superblocks...\n")

	superblock, err := fs.Superblock()
	if err != nil {
		return nil, fmt.Errorf("superblock: %w", err)
	}

	return superblock, nil
}
