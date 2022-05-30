package binstruct

import (
	"reflect"

	"lukeshu.com/btrfs-tools/pkg/binstruct/binint"
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

var (
	u8Type = reflect.TypeOf(U8(0))
	i8Type = reflect.TypeOf(I8(0))
)
