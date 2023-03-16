// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package binstruct implements simple struct-tag-based conversion
// between Go structures and binary on-disk representations of that
// data.
package binstruct

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct/binutil"
)

type Unmarshaler interface {
	UnmarshalBinary([]byte) (int, error)
}

func Unmarshal(dat []byte, dstPtr any) (int, error) {
	if unmar, ok := dstPtr.(Unmarshaler); ok {
		n, err := unmar.UnmarshalBinary(dat)
		if err != nil {
			err = &UnmarshalError{
				Type:   reflect.TypeOf(dstPtr),
				Method: "UnmarshalBinary",
				Err:    err,
			}
		}
		return n, err
	}
	return UnmarshalWithoutInterface(dat, dstPtr)
}

// unmarshal is like Unmarshal, but for internal use to avoid some
// slow round-tripping between `any` and `reflect.Value`.
func unmarshal(dat []byte, dst reflect.Value, isUnmarshaler bool) (int, error) {
	if isUnmarshaler {
		n, err := dst.Addr().Interface().(Unmarshaler).UnmarshalBinary(dat)
		if err != nil {
			err = &UnmarshalError{
				Type:   reflect.PtrTo(dst.Type()),
				Method: "UnmarshalBinary",
				Err:    err,
			}
		}
		return n, err
	}
	return unmarshalWithoutInterface(dat, dst)
}

func UnmarshalWithoutInterface(dat []byte, dstPtr any) (int, error) {
	_dstPtr := reflect.ValueOf(dstPtr)
	if _dstPtr.Kind() != reflect.Ptr {
		panic(&InvalidTypeError{
			Type: _dstPtr.Type(),
			Err:  errors.New("not a pointer"),
		})
	}
	return unmarshalWithoutInterface(dat, _dstPtr.Elem())
}

func unmarshalWithoutInterface(dat []byte, dst reflect.Value) (int, error) {
	switch dst.Kind() {
	case reflect.Uint8:
		if err := binutil.NeedNBytes(dat, sizeof8); err != nil {
			return 0, err
		}
		dst.SetUint(uint64(dat[0]))
		return sizeof8, nil
	case reflect.Int8:
		if err := binutil.NeedNBytes(dat, sizeof8); err != nil {
			return 0, err
		}
		dst.SetInt(int64(dat[0]))
		return sizeof8, nil
	case reflect.Uint16:
		if err := binutil.NeedNBytes(dat, sizeof16); err != nil {
			return 0, err
		}
		dst.SetUint(uint64(binary.LittleEndian.Uint16(dat[:sizeof16])))
		return sizeof16, nil
	case reflect.Int16:
		if err := binutil.NeedNBytes(dat, sizeof16); err != nil {
			return 0, err
		}
		dst.SetInt(int64(binary.LittleEndian.Uint16(dat[:sizeof16])))
		return sizeof16, nil
	case reflect.Uint32:
		if err := binutil.NeedNBytes(dat, sizeof32); err != nil {
			return 0, err
		}
		dst.SetUint(uint64(binary.LittleEndian.Uint32(dat[:sizeof32])))
		return sizeof32, nil
	case reflect.Int32:
		if err := binutil.NeedNBytes(dat, sizeof32); err != nil {
			return 0, err
		}
		dst.SetInt(int64(binary.LittleEndian.Uint32(dat[:sizeof32])))
		return sizeof32, nil
	case reflect.Uint64:
		if err := binutil.NeedNBytes(dat, sizeof64); err != nil {
			return 0, err
		}
		dst.SetUint(binary.LittleEndian.Uint64(dat[:sizeof64]))
		return sizeof64, nil
	case reflect.Int64:
		if err := binutil.NeedNBytes(dat, sizeof64); err != nil {
			return 0, err
		}
		dst.SetInt(int64(binary.LittleEndian.Uint64(dat[:sizeof64])))
		return sizeof64, nil
	case reflect.Ptr:
		typ := dst.Type()
		elemPtr := reflect.New(typ.Elem())
		n, err := unmarshal(dat, elemPtr.Elem(), typ.Implements(unmarshalerType))
		dst.SetPointer(elemPtr.UnsafePointer())
		return n, err
	case reflect.Array:
		isUnmarshaler := dst.Type().Elem().Implements(unmarshalerType)
		var n int
		for i := 0; i < dst.Len(); i++ {
			_n, err := unmarshal(dat[n:], dst.Index(i), isUnmarshaler)
			n += _n
			if err != nil {
				return n, err
			}
		}
		return n, nil
	case reflect.Struct:
		return getStructHandler(dst.Type()).Unmarshal(dat, dst)
	default:
		panic(&InvalidTypeError{
			Type: reflect.PtrTo(dst.Type()),
			Err: fmt.Errorf("does not implement binfmt.Unmarshaler and kind=%v is not a supported statically-sized kind",
				dst.Kind()),
		})
	}
}
