// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package util_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

type FmtState struct {
	MWidth     int
	MPrec      int
	MFlagMinus bool
	MFlagPlus  bool
	MFlagSharp bool
	MFlagSpace bool
	MFlagZero  bool
}

func (st FmtState) Width() (int, bool) {
	if st.MWidth < 1 {
		return 0, false
	}
	return st.MWidth, true
}

func (st FmtState) Precision() (int, bool) {
	if st.MPrec < 1 {
		return 0, false
	}
	return st.MPrec, true
}

func (st FmtState) Flag(b int) bool {
	switch b {
	case '-':
		return st.MFlagMinus
	case '+':
		return st.MFlagPlus
	case '#':
		return st.MFlagSharp
	case ' ':
		return st.MFlagSpace
	case '0':
		return st.MFlagZero
	}
	return false
}

func (st FmtState) Write([]byte) (int, error) {
	panic("not implemented")
}

func (dst *FmtState) Format(src fmt.State, verb rune) {
	if width, ok := src.Width(); ok {
		dst.MWidth = width
	}
	if prec, ok := src.Precision(); ok {
		dst.MPrec = prec
	}
	dst.MFlagMinus = src.Flag('-')
	dst.MFlagPlus = src.Flag('+')
	dst.MFlagSharp = src.Flag('#')
	dst.MFlagSpace = src.Flag(' ')
	dst.MFlagZero = src.Flag('0')
}

// letters only? No 'p', 'T', or 'w'.
const verbs = "abcdefghijklmnoqrstuvxyzABCDEFGHIJKLMNOPQRSUVWXYZ"

func FuzzFmtStateString(f *testing.F) {
	f.Fuzz(func(t *testing.T,
		width, prec uint8,
		flagMinus, flagPlus, flagSharp, flagSpace, flagZero bool,
		verbIdx uint8,
	) {
		if flagMinus {
			flagZero = false
		}
		input := FmtState{
			MWidth:     int(width),
			MPrec:      int(prec),
			MFlagMinus: flagMinus,
			MFlagPlus:  flagPlus,
			MFlagSharp: flagSharp,
			MFlagSpace: flagSpace,
			MFlagZero:  flagZero,
		}
		verb := rune(verbs[int(verbIdx)%len(verbs)])

		t.Logf("(%#v, %c) => %q", input, verb, util.FmtStateString(input, verb))

		var output FmtState
		assert.Equal(t, "", fmt.Sprintf(util.FmtStateString(input, verb), &output))
		assert.Equal(t, input, output)
	})
}
