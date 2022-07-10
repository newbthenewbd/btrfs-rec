// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"fmt"
	"os"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
)

func Open(flag int, filenames ...string) (*btrfs.FS, error) {
	fs := new(btrfs.FS)
	for _, filename := range filenames {
		fh, err := os.OpenFile(filename, flag, 0)
		if err != nil {
			_ = fs.Close()
			return nil, fmt.Errorf("file %q: %w", filename, err)
		}
		if err := fs.AddDevice(&btrfs.Device{File: fh}); err != nil {
			_ = fs.Close()
			return nil, fmt.Errorf("file %q: %w", filename, err)
		}
	}
	return fs, nil
}