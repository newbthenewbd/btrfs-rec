// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package binstruct

import (
	"reflect"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct/binint"
)

type (
	U8    = binint.U8
	U16le = binint.U16le
	U32le = binint.U32le
	U64le = binint.U64le
	U16be = binint.U16be
	U32be = binint.U32be
	U64be = binint.U64be
	I8    = binint.I8
	I16le = binint.I16le
	I32le = binint.I32le
	I64le = binint.I64le
	I16be = binint.I16be
	I32be = binint.I32be
	I64be = binint.I64be
)

var intKind2Type = map[reflect.Kind]reflect.Type{
	reflect.Uint8:  reflect.TypeOf(U8(0)),
	reflect.Int8:   reflect.TypeOf(I8(0)),
	reflect.Uint16: reflect.TypeOf(U16le(0)),
	reflect.Int16:  reflect.TypeOf(I16le(0)),
	reflect.Uint32: reflect.TypeOf(U32le(0)),
	reflect.Int32:  reflect.TypeOf(I32le(0)),
	reflect.Uint64: reflect.TypeOf(U64le(0)),
	reflect.Int64:  reflect.TypeOf(I64le(0)),
}
