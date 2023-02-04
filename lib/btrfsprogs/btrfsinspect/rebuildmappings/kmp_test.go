// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type bytePattern[K ~int64 | ~int] []byte

var _ kmpPattern[int, byte] = bytePattern[int]{}

// PatLen implements kmpPattern.
func (s bytePattern[K]) PatLen() K {
	return K(len(s))
}

// PatGet implements kmpPattern.
func (s bytePattern[K]) PatGet(i K) (byte, bool) {
	chr := s[int(i)]
	if chr == '.' {
		return 0, false
	}
	return chr, true
}

func TestBuildKMPTable(t *testing.T) {
	t.Parallel()
	substr := bytePattern[int64]([]byte("ababaa"))
	table := buildKMPTable[int64, byte](substr)
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
		table := buildKMPTable[int64, byte](bytePattern[int64](substr))
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
		act := indexAll[int64, byte](
			diskio.SliceSequence[int64, byte](str),
			bytePattern[int64](substr))
		assert.Equal(t, exp, act)
	})
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
			matches := indexAll[int64, byte](
				diskio.StringSequence[int64](tc.InStr),
				bytePattern[int64](tc.InSubstr))
			assert.Equal(t, tc.ExpMatches, matches)
		})
	}
}
