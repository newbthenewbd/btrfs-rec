// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfssum

import (
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"

	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
)

type CSum [0x20]byte

var (
	_ fmt.Stringer             = CSum{}
	_ fmt.Formatter            = CSum{}
	_ encoding.TextMarshaler   = CSum{}
	_ encoding.TextUnmarshaler = (*CSum)(nil)
)

func (csum CSum) String() string {
	return hex.EncodeToString(csum[:])
}

func (csum CSum) MarshalText() ([]byte, error) {
	var ret [len(csum) * 2]byte
	hex.Encode(ret[:], csum[:])
	return ret[:], nil
}

func (csum *CSum) UnmarshalText(text []byte) error {
	*csum = CSum{}
	_, err := hex.Decode(csum[:], text)
	return err
}

func (csum CSum) Fmt(typ CSumType) string {
	return hex.EncodeToString(csum[:typ.Size()])
}

func (csum CSum) Format(f fmt.State, verb rune) {
	fmtutil.FormatByteArrayStringer(csum, csum[:], f, verb)
}

type CSumType uint16

const (
	TYPE_CRC32 = CSumType(iota)
	TYPE_XXHASH
	TYPE_SHA256
	TYPE_BLAKE2
)

func (typ CSumType) String() string {
	names := map[CSumType]string{
		TYPE_CRC32:  "crc32c",
		TYPE_XXHASH: "xxhash64",
		TYPE_SHA256: "sha256",
		TYPE_BLAKE2: "blake2",
	}
	if name, ok := names[typ]; ok {
		return name
	}
	return fmt.Sprintf("%d", typ)
}

func (typ CSumType) Size() int {
	sizes := map[CSumType]int{
		TYPE_CRC32:  4,
		TYPE_XXHASH: 8,
		TYPE_SHA256: 32,
		TYPE_BLAKE2: 32,
	}
	if size, ok := sizes[typ]; ok {
		return size
	}
	return len(CSum{})
}

func (typ CSumType) Sum(data []byte) (CSum, error) {
	switch typ {
	case TYPE_CRC32:
		crc := crc32.Update(0, crc32.MakeTable(crc32.Castagnoli), data)

		var ret CSum
		binary.LittleEndian.PutUint32(ret[:], crc)
		return ret, nil
	case TYPE_XXHASH:
		panic("not implemented")
	case TYPE_SHA256:
		panic("not implemented")
	case TYPE_BLAKE2:
		panic("not implemented")
	default:
		return CSum{}, fmt.Errorf("unknown checksum type: %v", typ)
	}
}
