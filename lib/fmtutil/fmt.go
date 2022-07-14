// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package fmtutil

import (
	"fmt"
	"strings"
)

// FmtStateString returns the fmt.Printf string that produced a given
// fmt.State and verb.
func FmtStateString(st fmt.State, verb rune) string {
	var ret strings.Builder
	ret.WriteByte('%')
	for _, flag := range []int{'-', '+', '#', ' ', '0'} {
		if st.Flag(flag) {
			ret.WriteByte(byte(flag))
		}
	}
	if width, ok := st.Width(); ok {
		fmt.Fprintf(&ret, "%v", width)
	}
	if prec, ok := st.Precision(); ok {
		if prec == 0 {
			ret.WriteByte('.')
		} else {
			fmt.Fprintf(&ret, ".%v", prec)
		}
	}
	ret.WriteRune(verb)
	return ret.String()
}

// FormatByteArrayStringer is function for helping to implement
// fmt.Formatter for []byte or [n]byte types that have a custom string
// representation.  Use it like:
//
//     type MyType [16]byte
//
//     func (val MyType) String() string {
//         …
//     }
//
//     func (val MyType) Format(f fmt.State, verb rune) {
//         util.FormatByteArrayStringer(val, val[:], f, verb)
//     }
func FormatByteArrayStringer(
	obj interface {
		fmt.Stringer
		fmt.Formatter
	},
	objBytes []byte,
	f fmt.State, verb rune) {
	switch verb {
	case 'v':
		if !f.Flag('#') {
			FormatByteArrayStringer(obj, objBytes, f, 's') // as a string
		} else {
			byteStr := fmt.Sprintf("%#v", objBytes)
			objType := fmt.Sprintf("%T", obj)
			objStr := objType + strings.TrimPrefix(byteStr, "[]byte")
			fmt.Fprintf(f, FmtStateString(f, 's'), objStr)
		}
	case 's', 'q': // string
		fmt.Fprintf(f, FmtStateString(f, verb), obj.String())
	default:
		fmt.Fprintf(f, FmtStateString(f, verb), objBytes)
	}
}
