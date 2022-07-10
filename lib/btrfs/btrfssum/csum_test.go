// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfssum_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
)

func TestCSumFormat(t *testing.T) {
	t.Parallel()
	type TestCase struct {
		InputSum btrfssum.CSum
		InputFmt string
		Output   string
	}
	csum := btrfssum.CSum{0xbd, 0x7b, 0x41, 0xf4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	testcases := map[string]TestCase{
		"s":     {InputSum: csum, InputFmt: "%s", Output: "bd7b41f400000000000000000000000000000000000000000000000000000000"},
		"x":     {InputSum: csum, InputFmt: "%x", Output: "bd7b41f400000000000000000000000000000000000000000000000000000000"},
		"v":     {InputSum: csum, InputFmt: "%v", Output: "bd7b41f400000000000000000000000000000000000000000000000000000000"},
		"70s":   {InputSum: csum, InputFmt: "|% 70s", Output: "|      bd7b41f400000000000000000000000000000000000000000000000000000000"},
		"#180v": {InputSum: csum, InputFmt: "%#180v", Output: "   btrfssum.CSum{0xbd, 0x7b, 0x41, 0xf4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}"},
	}
	for tcName, tc := range testcases {
		tc := tc
		t.Run(tcName, func(t *testing.T) {
			t.Parallel()
			actual := fmt.Sprintf(tc.InputFmt, tc.InputSum)
			assert.Equal(t, tc.Output, actual)
		})
	}
}
