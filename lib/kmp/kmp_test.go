// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package kmp

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildTable(t *testing.T) {
	substr := []byte("ababaa")
	table := buildTable(substr)
	assert.Equal(t,
		[]int{0, 0, 1, 2, 3, 1},
		table)
	for j, val := range table {
		matchLen := j + 1
		assert.Equalf(t, substr[:val], substr[matchLen-val:matchLen],
			"for table[%d]=%d", j, val)
	}
}

func FuzzBuildTable(f *testing.F) {
	f.Add([]byte("ababaa"))
	f.Fuzz(func(t *testing.T, substr []byte) {
		table := buildTable(substr)
		assert.Equal(t, len(substr), len(table), "length")
		for j, val := range table {
			matchLen := j + 1
			assert.Equalf(t, substr[:val], substr[matchLen-val:matchLen],
				"for table[%d]=%d", j, val)
		}
	})
}

func NaiveFindAll(str, substr []byte) []int64 {
	var matches []int64
	for i := range str {
		if bytes.HasPrefix(str[i:], substr) {
			matches = append(matches, int64(i))
		}
	}
	return matches
}

func FuzzFindAll(f *testing.F) {
	f.Fuzz(func(t *testing.T, str, substr []byte) {
		if len(substr) == 0 {
			t.Skip()
		}
		t.Logf("str   =%q", str)
		t.Logf("substr=%q", substr)
		exp := NaiveFindAll(str, substr)
		act, err := FindAll(bytes.NewReader(str), substr)
		assert.NoError(t, err)
		assert.Equal(t, exp, act)
	})
}
