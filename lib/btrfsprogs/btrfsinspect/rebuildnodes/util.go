// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"golang.org/x/exp/constraints"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
)

func countNodes(nodeScanResults btrfsinspect.ScanDevicesResult) int {
	var cnt int
	for _, devResults := range nodeScanResults {
		cnt += len(devResults.FoundNodes)
	}
	return cnt
}

func roundDown[T constraints.Integer](n, d T) T {
	return (n / d) * d
}

func roundUp[T constraints.Integer](n, d T) T {
	return ((n + d - 1) / d) * d
}

func discardOK[T any](val T, _ bool) T {
	return val
}
