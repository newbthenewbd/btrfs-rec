// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"fmt"

	"golang.org/x/exp/constraints"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
)

func maybeSet[K, V comparable](name string, m map[K]V, k K, v V) error {
	if other, conflict := m[k]; conflict && other != v {
		return fmt.Errorf("conflict: %s %v can't have both %v and %v", name, k, other, v)
	}
	m[k] = v
	return nil
}

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
