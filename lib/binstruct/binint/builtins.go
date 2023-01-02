// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package binint

import (
	"encoding/binary"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct/binutil"
)

// unsigned

type U8 uint8

func (U8) BinaryStaticSize() int            { return 1 }
func (x U8) MarshalBinary() ([]byte, error) { return []byte{byte(x)}, nil }
func (x *U8) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 1); err != nil {
		return 0, err
	}
	*x = U8(dat[0])
	return 1, nil
}

// unsigned little endian

type U16le uint16

func (U16le) BinaryStaticSize() int { return 2 }
func (x U16le) MarshalBinary() ([]byte, error) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(x))
	return buf[:], nil
}

func (x *U16le) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 2); err != nil {
		return 0, err
	}
	*x = U16le(binary.LittleEndian.Uint16(dat))
	return 2, nil
}

type U32le uint32

func (U32le) BinaryStaticSize() int { return 4 }
func (x U32le) MarshalBinary() ([]byte, error) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(x))
	return buf[:], nil
}

func (x *U32le) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 4); err != nil {
		return 0, err
	}
	*x = U32le(binary.LittleEndian.Uint32(dat))
	return 4, nil
}

type U64le uint64

func (U64le) BinaryStaticSize() int { return 8 }
func (x U64le) MarshalBinary() ([]byte, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(x))
	return buf[:], nil
}

func (x *U64le) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 8); err != nil {
		return 0, err
	}
	*x = U64le(binary.LittleEndian.Uint64(dat))
	return 8, nil
}

// unsigned big endian

type U16be uint16

func (U16be) BinaryStaticSize() int { return 2 }
func (x U16be) MarshalBinary() ([]byte, error) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(x))
	return buf[:], nil
}

func (x *U16be) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 2); err != nil {
		return 0, err
	}
	*x = U16be(binary.BigEndian.Uint16(dat))
	return 2, nil
}

type U32be uint32

func (U32be) BinaryStaticSize() int { return 4 }
func (x U32be) MarshalBinary() ([]byte, error) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(x))
	return buf[:], nil
}

func (x *U32be) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 4); err != nil {
		return 0, err
	}
	*x = U32be(binary.BigEndian.Uint32(dat))
	return 4, nil
}

type U64be uint64

func (U64be) BinaryStaticSize() int { return 8 }
func (x U64be) MarshalBinary() ([]byte, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(x))
	return buf[:], nil
}

func (x *U64be) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 8); err != nil {
		return 0, err
	}
	*x = U64be(binary.BigEndian.Uint64(dat))
	return 8, nil
}

// signed

type I8 int8

func (I8) BinaryStaticSize() int            { return 1 }
func (x I8) MarshalBinary() ([]byte, error) { return []byte{byte(x)}, nil }
func (x *I8) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 1); err != nil {
		return 0, err
	}
	*x = I8(dat[0])
	return 1, nil
}

// signed little endian

type I16le int16

func (I16le) BinaryStaticSize() int { return 2 }
func (x I16le) MarshalBinary() ([]byte, error) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(x))
	return buf[:], nil
}

func (x *I16le) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 2); err != nil {
		return 0, err
	}
	*x = I16le(binary.LittleEndian.Uint16(dat))
	return 2, nil
}

type I32le int32

func (I32le) BinaryStaticSize() int { return 4 }
func (x I32le) MarshalBinary() ([]byte, error) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(x))
	return buf[:], nil
}

func (x *I32le) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 4); err != nil {
		return 0, err
	}
	*x = I32le(binary.LittleEndian.Uint32(dat))
	return 4, nil
}

type I64le int64

func (I64le) BinaryStaticSize() int { return 8 }
func (x I64le) MarshalBinary() ([]byte, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(x))
	return buf[:], nil
}

func (x *I64le) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 8); err != nil {
		return 0, err
	}
	*x = I64le(binary.LittleEndian.Uint64(dat))
	return 8, nil
}

// signed big endian

type I16be int16

func (I16be) BinaryStaticSize() int { return 2 }
func (x I16be) MarshalBinary() ([]byte, error) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(x))
	return buf[:], nil
}

func (x *I16be) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 2); err != nil {
		return 0, err
	}
	*x = I16be(binary.BigEndian.Uint16(dat))
	return 2, nil
}

type I32be int32

func (I32be) BinaryStaticSize() int { return 4 }
func (x I32be) MarshalBinary() ([]byte, error) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(x))
	return buf[:], nil
}

func (x *I32be) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 4); err != nil {
		return 0, err
	}
	*x = I32be(binary.BigEndian.Uint32(dat))
	return 4, nil
}

type I64be int64

func (I64be) BinaryStaticSize() int { return 8 }
func (x I64be) MarshalBinary() ([]byte, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(x))
	return buf[:], nil
}

func (x *I64be) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 8); err != nil {
		return 0, err
	}
	*x = I64be(binary.BigEndian.Uint64(dat))
	return 8, nil
}
