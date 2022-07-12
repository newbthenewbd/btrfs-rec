// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"context"
	"fmt"
	"os"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

func pass0(ctx context.Context, filenames ...string) (*btrfs.FS, *util.Ref[btrfsvol.PhysicalAddr, btrfs.Superblock], error) {
	fmt.Printf("\nPass 0: init and superblocks...\n")

	fs, err := btrfsutil.Open(ctx, os.O_RDWR, filenames...)
	if err != nil {
		return nil, nil, err
	}

	sb, err := fs.Superblock()
	if err != nil {
		_ = fs.Close()
		return nil, nil, err
	}

	return fs, sb, nil
}
