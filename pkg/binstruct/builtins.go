package binstruct

import (
	"encoding/binary"
	"fmt"
)

func needNBytes(t interface{}, dat []byte, n int) error {
	if len(dat) < n {
		return fmt.Errorf("%T.UnmarshalBinary: need at least %d bytes, only have %d", t, n, len(dat))
	}
	return nil
}

// unsigned

type u8 uint8

func (u8) BinaryStaticSize() int            { return 1 }
func (x u8) MarshalBinary() ([]byte, error) { return []byte{byte(x)}, nil }
func (x *u8) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 1); err != nil {
		return 0, err
	}
	*x = u8(dat[0])
	return 1, nil
}

// unsigned little endian

type u16le uint16

func (u16le) BinaryStaticSize() int { return 2 }
func (x u16le) MarshalBinary() ([]byte, error) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(x))
	return buf[:], nil
}
func (x *u16le) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 2); err != nil {
		return 0, err
	}
	*x = u16le(binary.LittleEndian.Uint16(dat))
	return 2, nil
}

type u32le uint32

func (u32le) BinaryStaticSize() int { return 4 }
func (x u32le) MarshalBinary() ([]byte, error) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(x))
	return buf[:], nil
}
func (x *u32le) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 4); err != nil {
		return 0, err
	}
	*x = u32le(binary.LittleEndian.Uint32(dat))
	return 2, nil
}

type u64le uint64

func (u64le) BinaryStaticSize() int { return 8 }
func (x u64le) MarshalBinary() ([]byte, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(x))
	return buf[:], nil
}
func (x *u64le) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 8); err != nil {
		return 0, err
	}
	*x = u64le(binary.LittleEndian.Uint64(dat))
	return 2, nil
}

// unsigned big endian

type u16be uint16

func (u16be) BinaryStaticSize() int { return 2 }
func (x u16be) MarshalBinary() ([]byte, error) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(x))
	return buf[:], nil
}
func (x *u16be) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 2); err != nil {
		return 0, err
	}
	*x = u16be(binary.BigEndian.Uint16(dat))
	return 2, nil
}

type u32be uint32

func (u32be) BinaryStaticSize() int { return 4 }
func (x u32be) MarshalBinary() ([]byte, error) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(x))
	return buf[:], nil
}
func (x *u32be) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 4); err != nil {
		return 0, err
	}
	*x = u32be(binary.BigEndian.Uint32(dat))
	return 2, nil
}

type u64be uint64

func (u64be) BinaryStaticSize() int { return 8 }
func (x u64be) MarshalBinary() ([]byte, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(x))
	return buf[:], nil
}
func (x *u64be) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 8); err != nil {
		return 0, err
	}
	*x = u64be(binary.BigEndian.Uint64(dat))
	return 2, nil
}

// signed

type i8 int8

func (i8) BinaryStaticSize() int            { return 1 }
func (x i8) MarshalBinary() ([]byte, error) { return []byte{byte(x)}, nil }
func (x *i8) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 1); err != nil {
		return 0, err
	}
	*x = i8(dat[0])
	return 1, nil
}

// signed little endian

type i16le int16

func (i16le) BinaryStaticSize() int { return 2 }
func (x i16le) MarshalBinary() ([]byte, error) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(x))
	return buf[:], nil
}
func (x *i16le) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 2); err != nil {
		return 0, err
	}
	*x = i16le(binary.LittleEndian.Uint16(dat))
	return 2, nil
}

type i32le int32

func (i32le) BinaryStaticSize() int { return 4 }
func (x i32le) MarshalBinary() ([]byte, error) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], uint32(x))
	return buf[:], nil
}
func (x *i32le) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 4); err != nil {
		return 0, err
	}
	*x = i32le(binary.LittleEndian.Uint32(dat))
	return 2, nil
}

type i64le int64

func (i64le) BinaryStaticSize() int { return 8 }
func (x i64le) MarshalBinary() ([]byte, error) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(x))
	return buf[:], nil
}
func (x *i64le) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 8); err != nil {
		return 0, err
	}
	*x = i64le(binary.LittleEndian.Uint64(dat))
	return 2, nil
}

// signed big endian

type i16be int16

func (i16be) BinaryStaticSize() int { return 2 }
func (x i16be) MarshalBinary() ([]byte, error) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(x))
	return buf[:], nil
}
func (x *i16be) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 2); err != nil {
		return 0, err
	}
	*x = i16be(binary.BigEndian.Uint16(dat))
	return 2, nil
}

type i32be int32

func (i32be) BinaryStaticSize() int { return 4 }
func (x i32be) MarshalBinary() ([]byte, error) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(x))
	return buf[:], nil
}
func (x *i32be) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 4); err != nil {
		return 0, err
	}
	*x = i32be(binary.BigEndian.Uint32(dat))
	return 2, nil
}

type i64be int64

func (i64be) BinaryStaticSize() int { return 8 }
func (x i64be) MarshalBinary() ([]byte, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(x))
	return buf[:], nil
}
func (x *i64be) UnmarshalBinary(dat []byte) (int, error) {
	if err := needNBytes(*x, dat, 8); err != nil {
		return 0, err
	}
	*x = i64be(binary.BigEndian.Uint64(dat))
	return 2, nil
}
