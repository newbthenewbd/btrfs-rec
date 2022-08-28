// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
)

func FuzzRoundTripNode(f *testing.F) {
	f.Fuzz(func(t *testing.T, inDat []byte) {
		t.Logf("dat=(%d)%q", len(inDat), inDat)
		node := btrfstree.Node{
			ChecksumType: btrfssum.TYPE_CRC32,
		}
		n, err := binstruct.Unmarshal(inDat, &node)
		if err != nil {
			t.Logf("err=%v", err)
			//require.Equal(t, 0, n)
		} else {
			require.Equal(t, len(inDat), n)

			outDat, err := binstruct.Marshal(node)
			require.NoError(t, err)
			require.Equal(t, inDat[:], outDat)
		}
	})
}
