// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildtrees

import (
	"golang.org/x/exp/constraints"

	"git.lukeshu.com/btrfs-progs-ng/cmd/btrfs-rec/inspect/rebuildmappings"
)

func countNodes(nodeScanResults rebuildmappings.ScanDevicesResult) int {
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
