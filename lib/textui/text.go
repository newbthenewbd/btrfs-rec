// Copyright (C) 2022-2024  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package textui implements utilities for emitting human-friendly
// text on stdout and stderr.
package textui

import (
	"fmt"
	"io"
	"math"
	"math/big"
	"unicode/utf8"

	"golang.org/x/exp/constraints"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"golang.org/x/text/number"

	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
)

var printer = message.NewPrinter(language.English)

// Fprintf is like `fmt.Fprintf`, but (1) includes the extensions of
// `golang.org/x/text/message.Printer`, and (2) is useful for marking
// when a print call is part of the UI, rather than something
// internal.
func Fprintf(w io.Writer, key string, a ...any) (n int, err error) {
	return printer.Fprintf(w, key, a...)
}

// Sprintf is like `fmt.Sprintf`, but (1) includes the extensions of
// `golang.org/x/text/message.Printer`, and (2) is useful for marking
// when a sprint call is part of the UI, rather than something
// internal.
func Sprintf(key string, a ...any) string {
	return printer.Sprintf(key, a...)
}

////////////////////////////////////////////////////////////////////////////////

// Humanized wraps a value such that formatting of it can make use of
// the `golang.org/x/text/message.Printer` extensions even when used
// with plain-old `fmt`.
func Humanized(x any) any {
	return humanized{val: x}
}

type humanized struct {
	val any
}

var (
	_ fmt.Formatter = humanized{}
	_ fmt.Stringer  = humanized{}
)

// String implements fmt.Formatter.
func (h humanized) Format(f fmt.State, verb rune) {
	_, _ = printer.Fprintf(f, fmtutil.FmtStateString(f, verb), h.val)
}

// String implements fmt.Stringer.
func (h humanized) String() string {
	return fmt.Sprint(h)
}

////////////////////////////////////////////////////////////////////////////////

// Portion renders a fraction N/D as both a percentage and
// parenthetically as the exact fractional value, rendered with
// human-friendly commas.
//
// For example:
//
//	fmt.Sprint(Portion[int]{N: 1, D: 12345}) ⇒ "0% (1/12,345)"
type Portion[T constraints.Integer] struct {
	N, D T
}

var _ fmt.Stringer = Portion[int]{}

// String implements fmt.Stringer.
func (p Portion[T]) String() string {
	pct := uint64(100)
	if p.D > 0 {
		pct = (uint64(p.N) * 100) / uint64(p.D)
	}
	return printer.Sprintf("%d%% (%v/%v)", pct, uint64(p.N), uint64(p.D))
}

////////////////////////////////////////////////////////////////////////////////

// toRat(x) returns `x` as a [*big.Rat], or `nil` if `x` is NaN.
func toRat[T constraints.Integer | constraints.Float | *big.Int | *big.Float | *big.Rat](x T) *big.Rat {
	var y *big.Rat
	switch x := any(x).(type) {
	case *big.Rat:
		y = new(big.Rat).Set(x)
	case *big.Float:
		y, _ = x.Rat(nil)
	case *big.Int:
		y = new(big.Rat).SetInt(x)

	case uint:
		y = new(big.Rat).SetUint64(uint64(x))
	case uint8:
		y = new(big.Rat).SetUint64(uint64(x))
	case uint16:
		y = new(big.Rat).SetUint64(uint64(x))
	case uint32:
		y = new(big.Rat).SetUint64(uint64(x))
	case uint64:
		y = new(big.Rat).SetUint64(x)
	case uintptr:
		y = new(big.Rat).SetUint64(uint64(x))

	case int:
		y = new(big.Rat).SetInt64(int64(x))
	case int8:
		y = new(big.Rat).SetInt64(int64(x))
	case int16:
		y = new(big.Rat).SetInt64(int64(x))
	case int32:
		y = new(big.Rat).SetInt64(int64(x))
	case int64:
		y = new(big.Rat).SetInt64(x)

	case float32:
		if math.IsNaN(float64(x)) {
			y = nil
		} else {
			y, _ = big.NewFloat(float64(x)).Rat(nil)
		}
	case float64:
		if math.IsNaN(x) {
			y = nil
		} else {
			y, _ = big.NewFloat(x).Rat(nil)
		}

	default:
		panic(fmt.Errorf("should not happen: unmatched type %T", x))
	}
	return y
}

func formatFloatWithSuffix(f fmt.State, verb rune, val float64, suffix string) {
	var wrapped any = val // float64 or number.Decimal[float64]
	if !math.IsNaN(val) {
		var options []number.Option
		if width, ok := f.Width(); ok {
			width -= utf8.RuneCountInString(suffix)
			options = append(options, number.FormatWidth(width))
		}
		if prec, ok := f.Precision(); ok {
			options = append(options, number.Precision(prec))
		}
		wrapped = number.Decimal(val, options...)
	}
	var format string
	if width, ok := f.Width(); ok {
		width -= utf8.RuneCountInString(suffix)
		format = fmtutil.FmtStateStringWidth(f, verb, width)
	} else {
		format = fmtutil.FmtStateString(f, verb)
	}
	_, _ = printer.Fprintf(f, format+"%s",
		wrapped, suffix)
}

////////////////////////////////////////////////////////////////////////////////

type metric struct {
	Val  *big.Rat
	Unit string
}

var (
	_ fmt.Formatter = metric{}
	_ fmt.Stringer  = metric{}
)

func Metric[T constraints.Integer | constraints.Float | *big.Int | *big.Float | *big.Rat](x T, unit string) metric {
	return metric{
		Val:  toRat(x),
		Unit: unit,
	}
}

var metricSmallPrefixes = []string{
	"m",
	"μ",
	"n",
	"p",
	"f",
	"a",
	"z",
	"y",
	"r",
	"q",
}

var metricBigPrefixes = []string{
	"k",
	"M",
	"G",
	"T",
	"P",
	"E",
	"Z",
	"Y",
	"R",
	"Q",
}

var (
	one     = big.NewRat(1, 1)
	kilo    = big.NewRat(1000, 1)
	kiloInv = new(big.Rat).Inv(kilo)
)

func lt(a, b *big.Rat) bool {
	return a.Cmp(b) < 0
}

func gte(a, b *big.Rat) bool {
	return a.Cmp(b) >= 0
}

// String implements fmt.Formatter.
func (v metric) Format(f fmt.State, verb rune) {
	var prefix string
	var float float64
	if v.Val == nil {
		float = math.NaN()
	} else {
		rat := new(big.Rat).Abs(v.Val)
		if lt(rat, one) {
			for i := 0; lt(rat, one) && i < len(metricSmallPrefixes); i++ {
				rat.Mul(rat, kilo)
				prefix = metricSmallPrefixes[i]
			}
		} else {
			for i := 0; gte(rat, kilo) && i < len(metricBigPrefixes); i++ {
				rat.Mul(rat, kiloInv)
				prefix = metricBigPrefixes[i]
			}
		}
		if v.Val.Sign() < 0 {
			rat.Neg(rat)
		}
		float, _ = rat.Float64()
	}
	formatFloatWithSuffix(f, verb, float, prefix+v.Unit)
}

// String implements fmt.Stringer.
func (v metric) String() string {
	return fmt.Sprint(v)
}

////////////////////////////////////////////////////////////////////////////////

type iec struct {
	Val  *big.Rat
	Unit string
}

var (
	_ fmt.Formatter = iec{}
	_ fmt.Stringer  = iec{}
)

func IEC[T constraints.Integer | constraints.Float | *big.Int | *big.Float | *big.Rat](x T, unit string) iec {
	return iec{
		Val:  toRat(x),
		Unit: unit,
	}
}

var iecPrefixes = []string{
	"Ki",
	"Mi",
	"Gi",
	"Ti",
	"Pi",
	"Ei",
	"Zi",
	"Yi",
}

var (
	kibi    = big.NewRat(1024, 1)
	kibiInv = new(big.Rat).Inv(kibi)
)

// String implements fmt.Formatter.
func (v iec) Format(f fmt.State, verb rune) {
	var prefix string
	var float float64
	if v.Val == nil {
		float = math.NaN()
	} else {
		rat := new(big.Rat).Abs(v.Val)
		for i := 0; gte(rat, kibi) && i < len(iecPrefixes); i++ {
			rat.Mul(rat, kibiInv)
			prefix = iecPrefixes[i]
		}
		if v.Val.Sign() < 0 {
			rat.Neg(rat)
		}
		float, _ = rat.Float64()
	}
	formatFloatWithSuffix(f, verb, float, prefix+v.Unit)
}

// String implements fmt.Stringer.
func (v iec) String() string {
	return fmt.Sprint(v)
}
