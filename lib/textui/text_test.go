// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package textui_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func TestFprintf(t *testing.T) {
	t.Parallel()
	var out strings.Builder
	textui.Fprintf(&out, "%d", 12345)
	assert.Equal(t, "12,345", out.String())
}

func TestHumanized(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "12,345", fmt.Sprint(textui.Humanized(12345)))
	assert.Equal(t, "12,345  ", fmt.Sprintf("%-8d", textui.Humanized(12345)))

	laddr := btrfsvol.LogicalAddr(345243543)
	assert.Equal(t, "0x000000001493ff97", fmt.Sprintf("%v", textui.Humanized(laddr)))
	assert.Equal(t, "345243543", fmt.Sprintf("%d", textui.Humanized(laddr)))
	assert.Equal(t, "345,243,543", fmt.Sprintf("%d", textui.Humanized(uint64(laddr))))
}

func TestPortion(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "100% (0/0)", fmt.Sprint(textui.Portion[int]{}))
	assert.Equal(t, "0% (1/12,345)", fmt.Sprint(textui.Portion[int]{N: 1, D: 12345}))
	assert.Equal(t, "100% (0/0)", fmt.Sprint(textui.Portion[btrfsvol.PhysicalAddr]{}))
	assert.Equal(t, "0% (1/12,345)", fmt.Sprint(textui.Portion[btrfsvol.PhysicalAddr]{N: 1, D: 12345}))
}
