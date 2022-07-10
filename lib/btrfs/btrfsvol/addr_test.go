// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsvol_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

func TestAddrFormat(t *testing.T) {
	t.Parallel()
	type TestCase struct {
		InputAddr btrfsvol.LogicalAddr
		InputFmt  string
		Output    string
	}
	addr := btrfsvol.LogicalAddr(0x3a41678000)
	testcases := map[string]TestCase{
		"v":   {InputAddr: addr, InputFmt: "%v", Output: "0x0000003a41678000"},
		"s":   {InputAddr: addr, InputFmt: "%s", Output: "0x0000003a41678000"},
		"q":   {InputAddr: addr, InputFmt: "%q", Output: `"0x0000003a41678000"`},
		"x":   {InputAddr: addr, InputFmt: "%x", Output: "3a41678000"},
		"d":   {InputAddr: addr, InputFmt: "%d", Output: "250205405184"},
		"neg": {InputAddr: -1, InputFmt: "%v", Output: "-0x000000000000001"},
	}
	for tcName, tc := range testcases {
		tc := tc
		t.Run(tcName, func(t *testing.T) {
			t.Parallel()
			actual := fmt.Sprintf(tc.InputFmt, tc.InputAddr)
			assert.Equal(t, tc.Output, actual)
		})
	}
}
