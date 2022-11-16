// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
)

func maybeSet[K, V comparable](name string, m map[K]V, k K, v V) error {
	if other, conflict := m[k]; conflict && other != v {
		return fmt.Errorf("conflict: %s %v can't have both %v and %v", name, k, other, v)
	}
	m[k] = v
	return nil
}

/*
var maxKey = btrfsprim.Key{
	ObjectID: math.MaxUint64,
	ItemType: math.MaxUint8,
	Offset:   math.MaxUint64,
}

func keyMm(key btrfsprim.Key) btrfsprim.Key {
	switch {
	case key.Offset > 0:
		key.Offset--
	case key.ItemType > 0:
		key.ItemType--
	case key.ObjectID > 0:
		key.ObjectID--
	}
	return key
}
*/

func countNodes(nodeScanResults btrfsinspect.ScanDevicesResult) int {
	var cnt int
	for _, devResults := range nodeScanResults {
		cnt += len(devResults.FoundNodes)
	}
	return cnt
}
