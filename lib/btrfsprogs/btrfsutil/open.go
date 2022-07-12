// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"
	"os"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
)

func Open(ctx context.Context, flag int, filenames ...string) (*btrfs.FS, error) {
	fs := new(btrfs.FS)
	for i, filename := range filenames {
		dlog.Debugf(ctx, "Adding device file %d/%d %q...", i, len(filenames), filename)
		fh, err := os.OpenFile(filename, flag, 0)
		if err != nil {
			_ = fs.Close()
			return nil, fmt.Errorf("device file %q: %w", filename, err)
		}
		if err := fs.AddDevice(ctx, &btrfs.Device{File: fh}); err != nil {
			return nil, fmt.Errorf("device file %q: %w", filename, err)
		}
	}
	return fs, nil
}
