// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package util

import (
	"fmt"
	"strings"
)

type BitfieldFormat uint8

const (
	HexNone = BitfieldFormat(iota)
	HexLower
	HexUpper
)

func BitfieldString[T ~uint8 | ~uint16 | ~uint32 | ~uint64](bitfield T, bitnames []string, cfg BitfieldFormat) string {
	var out strings.Builder
	switch cfg {
	case HexNone:
		// do nothing
	case HexLower:
		fmt.Fprintf(&out, "0x%0x(", uint64(bitfield))
	case HexUpper:
		fmt.Fprintf(&out, "0x%0X(", uint64(bitfield))
	}
	if bitfield == 0 {
		out.WriteString("none")
	} else {
		rest := bitfield
		first := true
		for i := 0; rest != 0; i++ {
			if rest&(1<<i) != 0 {
				if !first {
					out.WriteRune('|')
				}
				if i < len(bitnames) {
					out.WriteString(bitnames[i])
				} else {
					fmt.Fprintf(&out, "(1<<%v)", i)
				}
				first = false
			}
			rest &^= 1 << i
		}
	}
	if cfg != HexNone {
		out.WriteRune(')')
	}
	return out.String()
}
