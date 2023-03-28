// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"
	"os"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func Open(ctx context.Context, flag int, filenames ...string) (*btrfs.FS, error) {
	fs := new(btrfs.FS)
	for i, filename := range filenames {
		dlog.Debugf(ctx, "Adding device file %d/%d %q...", i, len(filenames), filename)
		osFile, err := os.OpenFile(filename, flag, 0)
		if err != nil {
			_ = fs.Close()
			return nil, fmt.Errorf("device file %q: %w", filename, err)
		}
		typedFile := &diskio.OSFile[btrfsvol.PhysicalAddr]{
			File: osFile,
		}
		bufFile := diskio.NewBufferedFile[btrfsvol.PhysicalAddr](
			ctx,
			typedFile,
			//nolint:gomnd // False positive: gomnd.ignored-functions=[textui.Tunable] doesn't support type params.
			textui.Tunable[btrfsvol.PhysicalAddr](16*1024), // block size: 16KiB
			textui.Tunable(1024),                           // number of blocks to buffer; total of 16MiB
		)
		devFile := &btrfs.Device{
			File: bufFile,
		}
		if err := fs.AddDevice(ctx, devFile); err != nil {
			return nil, fmt.Errorf("device file %q: %w", filename, err)
		}
	}
	return fs, nil
}
