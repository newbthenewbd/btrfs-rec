// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package diskio

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildKMPTable(t *testing.T) {
	substr := SliceSequence[int64, byte]([]byte("ababaa"))
	table, err := buildKMPTable[int64, byte](substr)
	require.NoError(t, err)
	require.Equal(t,
		[]int64{0, 0, 1, 2, 3, 1},
		table)
	for j, val := range table {
		matchLen := j + 1
		assert.Equalf(t, substr[:val], substr[matchLen-int(val):matchLen],
			"for table[%d]=%d", j, val)
	}
}

func FuzzBuildKMPTable(f *testing.F) {
	f.Add([]byte("ababaa"))
	f.Fuzz(func(t *testing.T, substr []byte) {
		table, err := buildKMPTable[int64, byte](SliceSequence[int64, byte](substr))
		require.NoError(t, err)
		require.Equal(t, len(substr), len(table), "length")
		for j, val := range table {
			matchLen := j + 1
			assert.Equalf(t, substr[:val], substr[matchLen-int(val):matchLen],
				"for table[%d]=%d", j, val)
		}
	})
}

func NaiveIndexAll(str, substr []byte) []int64 {
	var matches []int64
	for i := range str {
		if bytes.HasPrefix(str[i:], substr) {
			matches = append(matches, int64(i))
		}
	}
	return matches
}

func FuzzIndexAll(f *testing.F) {
	f.Fuzz(func(t *testing.T, str, substr []byte) {
		if len(substr) == 0 {
			t.Skip()
		}
		t.Logf("str   =%q", str)
		t.Logf("substr=%q", substr)
		exp := NaiveIndexAll(str, substr)
		act, err := IndexAll[int64, byte](
			&ByteReaderSequence[int64]{R: bytes.NewReader(str)},
			SliceSequence[int64, byte](substr))
		assert.NoError(t, err)
		assert.Equal(t, exp, act)
	})
}
