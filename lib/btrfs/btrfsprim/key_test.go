// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsprim

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func k(objID ObjID, typ ItemType, offset uint64) Key {
	return Key{
		ObjectID: objID,
		ItemType: typ,
		Offset:   offset,
	}
}

func eq(t *testing.T, act, exp Key) {
	t.Helper()
	assert.Equal(t, exp, act)
}

func ppEq(t *testing.T, in, exp Key) {
	t.Helper()
	eq(t, in.Pp(), exp)
	if in != MaxKey {
		eq(t, exp.Mm(), in)
	}
}

func mmEq(t *testing.T, in, exp Key) {
	t.Helper()
	eq(t, in.Mm(), exp)
	if in != (Key{}) {
		eq(t, exp.Pp(), in)
	}
}

func TestKey(t *testing.T) {
	t.Parallel()

	eq(t, MaxKey, k(18446744073709551615, 255, 18446744073709551615))

	// pp
	ppEq(t, k(0, 0, 0), k(0, 0, 1))
	ppEq(t, k(0, 0, 18446744073709551615), k(0, 1, 0))
	ppEq(t, k(0, 255, 0), k(0, 255, 1))
	ppEq(t, k(0, 255, 18446744073709551615), k(1, 0, 0))
	ppEq(t, MaxKey, k(18446744073709551615, 255, 18446744073709551615))

	// mm
	mmEq(t, MaxKey, k(18446744073709551615, 255, 18446744073709551614))
	mmEq(t, k(18446744073709551615, 255, 0), k(18446744073709551615, 254, 18446744073709551615))
	mmEq(t, k(18446744073709551615, 0, 0), k(18446744073709551614, 255, 18446744073709551615))
	mmEq(t, k(0, 0, 0), k(0, 0, 0))
}
