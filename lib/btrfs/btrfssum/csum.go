// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
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

const BlockSize = 4 * 1024

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
	TYPE_CRC32 CSumType = iota
	TYPE_XXHASH
	TYPE_SHA256
	TYPE_BLAKE2
)

var csumTypeNames = []string{
	"crc32c",
	"xxhash64",
	"sha256",
	"blake2",
}

var csumTypeSizes = []int{
	4,
	8,
	32,
	32,
}

func (typ CSumType) String() string {
	if int(typ) < len(csumTypeNames) {
		return csumTypeNames[typ]
	}
	return fmt.Sprintf("%d", typ)
}

func (typ CSumType) Size() int {
	if int(typ) < len(csumTypeSizes) {
		return csumTypeSizes[typ]
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
		panic("TODO: XXHASH not yet implemented")
	case TYPE_SHA256:
		panic("TODO: SHA256 not yet implemented")
	case TYPE_BLAKE2:
		panic("TODO: BLAKE2 not yet implemented")
	default:
		return CSum{}, fmt.Errorf("unknown checksum type: %v", typ)
	}
}
