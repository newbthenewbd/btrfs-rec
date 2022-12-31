// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package textui

import (
	"fmt"
	"io"
	"math"

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
	printer.Fprintf(f, fmtutil.FmtStateString(f, verb), h.val)
}

// String implements fmt.Stringer.
func (h humanized) String() string {
	return fmt.Sprint(h)
}

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

var (
	_ fmt.Stringer = Portion[int]{}
)

// String implements fmt.Stringer.
func (p Portion[T]) String() string {
	pct := float64(1)
	if p.D > 0 {
		pct = float64(p.N) / float64(p.D)
	}
	return printer.Sprintf("%v (%v/%v)", number.Percent(pct), uint64(p.N), uint64(p.D))
}

type metric[T constraints.Integer | constraints.Float] struct {
	Val  T
	Unit string
}

var (
	_ fmt.Formatter = metric[int]{}
	_ fmt.Stringer  = metric[int]{}
)

func Metric[T constraints.Integer | constraints.Float](x T, unit string) metric[T] {
	return metric[T]{
		Val:  x,
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

// String implements fmt.Formatter.
func (v metric[T]) Format(f fmt.State, verb rune) {
	var prefix string
	y := math.Abs(float64(v.Val))
	if y < 1 {
		for i := 0; y < 1 && i <= len(metricSmallPrefixes); i++ {
			y *= 1000
			prefix = metricSmallPrefixes[i]
		}
	} else {
		for i := 0; y > 1000 && i <= len(metricBigPrefixes); i++ {
			y /= 1000
			prefix = metricBigPrefixes[i]
		}
	}
	if v.Val < 0 {
		y = -y
	}
	printer.Fprintf(f, fmtutil.FmtStateString(f, verb)+"%s%s",
		y, prefix, v.Unit)
}

// String implements fmt.Stringer.
func (v metric[T]) String() string {
	return fmt.Sprint(v)
}

type iec[T constraints.Integer | constraints.Float] struct {
	Val  T
	Unit string
}

var (
	_ fmt.Formatter = iec[int]{}
	_ fmt.Stringer  = iec[int]{}
)

func IEC[T constraints.Integer | constraints.Float](x T, unit string) iec[T] {
	return iec[T]{
		Val:  x,
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

// String implements fmt.Formatter.
func (v iec[T]) Format(f fmt.State, verb rune) {
	var prefix string
	y := math.Abs(float64(v.Val))
	for i := 0; y > 1024 && i <= len(iecPrefixes); i++ {
		y /= 1024
		prefix = iecPrefixes[i]
	}
	if v.Val < 0 {
		y = -y
	}
	printer.Fprintf(f, fmtutil.FmtStateString(f, verb)+"%s%s",
		number.Decimal(y), prefix, v.Unit)
}

// String implements fmt.Stringer.
func (v iec[T]) String() string {
	return fmt.Sprint(v)
}
