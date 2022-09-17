// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
)

func buildUUIDMap(ctx context.Context, fs *btrfs.FS, scanResults btrfsinspect.ScanDevicesResult) (uuidMap, error) {
	ret, err := ScanDevices(ctx, fs, scanResults)
	if err != nil {
		return uuidMap{}, err
	}
	return ret.uuidMap, nil
}
