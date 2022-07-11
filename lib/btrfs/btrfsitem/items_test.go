// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/internal"
)

func FuzzRoundTrip(f *testing.F) {
	keySize := binstruct.StaticSize(internal.Key{})
	sumtypeSize := binstruct.StaticSize(btrfssum.CSumType(0))

	f.Add(make([]byte, 256))

	f.Fuzz(func(t *testing.T, inDat []byte) {
		if len(inDat) < keySize+sumtypeSize {
			t.Skip()
		}
		keyInDat, inDat := inDat[:keySize], inDat[keySize:]
		sumtypeInDat, inDat := inDat[:sumtypeSize], inDat[sumtypeSize:]
		itemInDat := inDat

		// key

		var key internal.Key
		n, err := binstruct.Unmarshal(keyInDat, &key)
		require.NoError(t, err, "binstruct.Unmarshal(dat, &key)")
		require.Equal(t, keySize, n, "binstruct.Unmarshal(dat, &key)")

		keyOutDat, err := binstruct.Marshal(key)
		require.NoError(t, err, "binstruct.Marshal(key)")
		require.Equal(t, keyInDat, keyOutDat, "binstruct.Marshal(key)")

		// sumtype

		var sumtype btrfssum.CSumType
		n, err = binstruct.Unmarshal(sumtypeInDat, &sumtype)
		require.NoError(t, err, "binstruct.Unmarshal(dat, &sumtype)")
		require.Equal(t, sumtypeSize, n, "binstruct.Unmarshal(dat, &sumtype)")

		sumtypeOutDat, err := binstruct.Marshal(sumtype)
		require.NoError(t, err, "binstruct.Marshal(sumtype)")
		require.Equal(t, sumtypeInDat, sumtypeOutDat, "binstruct.Marshal(sumtype)")

		// item

		t.Logf("key=%v sumtype=%v dat=%q", key, sumtype, itemInDat)

		item := btrfsitem.UnmarshalItem(key, sumtype, itemInDat)
		require.NotNil(t, item, "btrfsitem.UnmarshalItem")

		itemOutDat, err := binstruct.Marshal(item)
		require.NoError(t, err, "binstruct.Marshal(item)")
		require.Equal(t, string(itemInDat), string(itemOutDat), "binstruct.Marshal(item)")
	})
}
