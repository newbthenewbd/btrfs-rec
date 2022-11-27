// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"io"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
)

func ShowLoops(ctx context.Context, out io.Writer, fs *btrfs.FS, nodeScanResults btrfsinspect.ScanDevicesResult) error {
	scanData, err := ScanDevices(ctx, fs, nodeScanResults)
	if err != nil {
		return err
	}

	dlog.Info(ctx, "Walking graph...")
	scanData.nodeGraph.ShowLoops(out)

	return nil
}
