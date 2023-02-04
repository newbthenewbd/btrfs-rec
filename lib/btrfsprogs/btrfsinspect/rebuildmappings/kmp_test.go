// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

func TestBuildKMPTable(t *testing.T) {
	t.Parallel()
	substr := diskio.SliceSequence[int64, byte]([]byte("ababaa"))
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
		table, err := buildKMPTable[int64, byte](diskio.SliceSequence[int64, byte](substr))
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
			&diskio.ByteReaderSequence[int64]{R: bytes.NewReader(str)},
			diskio.SliceSequence[int64, byte](substr))
		assert.NoError(t, err)
		assert.Equal(t, exp, act)
	})
}

type RESeq string

func (re RESeq) Get(i int64) (byte, error) {
	if i < 0 || i >= int64(len(re)) {
		return 0, io.EOF
	}
	chr := re[int(i)]
	if chr == '.' {
		return 0, ErrWildcard
	}
	return chr, nil
}

func TestKMPWildcard(t *testing.T) {
	t.Parallel()
	type testcase struct {
		InStr      string
		InSubstr   string
		ExpMatches []int64
	}
	testcases := map[string]testcase{
		"trivial-bar": {
			InStr:      "foo_bar",
			InSubstr:   "foo.ba.",
			ExpMatches: []int64{0},
		},
		"trival-baz": {
			InStr:      "foo-baz",
			InSubstr:   "foo.ba.",
			ExpMatches: []int64{0},
		},
		"suffix": {
			InStr:      "foobarbaz",
			InSubstr:   "...baz",
			ExpMatches: []int64{3},
		},
		"overlap": {
			InStr:      "foobarbar",
			InSubstr:   "...bar",
			ExpMatches: []int64{0, 3},
		},
	}
	for tcName, tc := range testcases {
		tc := tc
		t.Run(tcName, func(t *testing.T) {
			t.Parallel()
			matches, err := IndexAll[int64, byte](
				diskio.StringSequence[int64](tc.InStr),
				RESeq(tc.InSubstr))
			assert.NoError(t, err)
			assert.Equal(t, tc.ExpMatches, matches)
		})
	}
}
