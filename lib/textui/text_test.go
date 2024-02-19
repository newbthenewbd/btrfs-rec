// Copyright (C) 2022-2024  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package textui_test

import (
	"fmt"
	"math"
	"math/big"
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

func TestMetric(t *testing.T) {
	t.Parallel()

	// _1e(n) returns `1e{n}`
	_1e := func(n int64) *big.Int {
		return new(big.Int).Exp(big.NewInt(10), big.NewInt(n), nil)
	}

	// _1e(n) returns `1e-{n}`
	_1em := func(n int64) *big.Rat {
		ret := new(big.Rat).SetInt(_1e(n))
		ret.Inv(ret)
		return ret
	}

	// I've flipped the "actual" end "expected" fields for these
	// tests, so that it's more readable as a table.

	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(33), "s")), "1,000Qs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(32), "s")), "100Qs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(31), "s")), "10Qs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(30), "s")), "1Qs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(29), "s")), "100Rs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(28), "s")), "10Rs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(27), "s")), "1Rs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(26), "s")), "100Ys")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(25), "s")), "10Ys")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(24), "s")), "1Ys")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(23), "s")), "100Zs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(22), "s")), "10Zs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(21), "s")), "1Zs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Int](_1e(20), "s")), "100Es")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e19, "s")), "10Es")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e18, "s")), "1Es")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e17, "s")), "100Ps")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e16, "s")), "10Ps")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e15, "s")), "1Ps")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e14, "s")), "100Ts")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e13, "s")), "10Ts")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e12, "s")), "1Ts")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e11, "s")), "100Gs")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e10, "s")), "10Gs")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e9, "s")), "1Gs")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e8, "s")), "100Ms")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e7, "s")), "10Ms")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e6, "s")), "1Ms")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e5, "s")), "100ks")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e4, "s")), "10ks")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e3, "s")), "1ks")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e2, "s")), "100s")
	assert.Equal(t, fmt.Sprint(textui.Metric[uint64](1e1, "s")), "10s")

	assert.Equal(t, fmt.Sprint(textui.Metric(1e0, "s")), "1s")

	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(1), "s")), "100ms")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(2), "s")), "10ms")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(3), "s")), "1ms")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(4), "s")), "100μs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(5), "s")), "10μs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(6), "s")), "1μs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(7), "s")), "100ns")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(8), "s")), "10ns")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(9), "s")), "1ns")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(10), "s")), "100ps")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(11), "s")), "10ps")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(12), "s")), "1ps")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(13), "s")), "100fs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(14), "s")), "10fs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(15), "s")), "1fs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(16), "s")), "100as")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(17), "s")), "10as")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(18), "s")), "1as")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(19), "s")), "100zs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(20), "s")), "10zs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(21), "s")), "1zs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(22), "s")), "100ys")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(23), "s")), "10ys")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(24), "s")), "1ys")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(25), "s")), "100rs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(26), "s")), "10rs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(27), "s")), "1rs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(28), "s")), "100qs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(29), "s")), "10qs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(30), "s")), "1qs")
	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(31), "s")), "0.1qs")

	assert.Equal(t, fmt.Sprint(textui.Metric[*big.Rat](_1em(31), "s")), "0.1qs")

	assert.Equal(t, fmt.Sprint(textui.Metric(math.NaN(), "s")), "NaNs")
	assert.Equal(t, fmt.Sprintf("%5.f", textui.Metric(1, "s")), "   1s")
	assert.Equal(t, fmt.Sprintf("%5.f", textui.Metric(1000, "s")), "  1ks")
	assert.Equal(t, fmt.Sprintf("%5.f", textui.Metric(_1em(6), "s")), "  1μs")

}

func TestIEC(t *testing.T) {
	t.Parallel()

	// _1ll(n) returns `1<<{n}`
	_1ll := func(n int) *big.Int {
		bs := make([]byte, 1+(n/8)) // = ⌈(n+1)/8⌉ = ((n+1)+(8-1))/8 = (n+8)/8 = 1+(n/8)
		bs[0] = 1 << (n % 8)
		return new(big.Int).SetBytes(bs)
	}

	// I've flipped the "actual" end "expected" fields for these
	// tests, so that it's more readable as a table.

	assert.Equal(t, fmt.Sprint(textui.IEC[*big.Int](_1ll(90), "B")), "1,024YiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[*big.Int](_1ll(85), "B")), "32YiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[*big.Int](_1ll(80), "B")), "1YiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[*big.Int](_1ll(75), "B")), "32ZiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[*big.Int](_1ll(70), "B")), "1ZiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[*big.Int](_1ll(65), "B")), "32EiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[uint64](1<<60, "B")), "1EiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[uint64](1<<55, "B")), "32PiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[uint64](1<<50, "B")), "1PiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[uint64](1<<45, "B")), "32TiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[uint64](1<<40, "B")), "1TiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[uint64](1<<35, "B")), "32GiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[uint64](1<<30, "B")), "1GiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[uint64](1<<25, "B")), "32MiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[uint64](1<<20, "B")), "1MiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[uint64](1<<15, "B")), "32KiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[uint64](1<<10, "B")), "1KiB")
	assert.Equal(t, fmt.Sprint(textui.IEC[uint64](1<<5, "B")), "32B")

	assert.Equal(t, fmt.Sprint(textui.IEC(1<<0, "B")), "1B")

	assert.Equal(t, fmt.Sprint(textui.IEC(math.NaN(), "B")), "NaNB")
	assert.Equal(t, fmt.Sprintf("%5.f", textui.IEC(1, "B")), "   1B")
	assert.Equal(t, fmt.Sprintf("%5.f", textui.IEC(1024, "B")), " 1KiB")
}
