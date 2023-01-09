// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

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

func UnmarshalWithoutInterface(dat []byte, dstPtr any) (int, error) {
	_dstPtr := reflect.ValueOf(dstPtr)
	if _dstPtr.Kind() != reflect.Ptr {
		panic(&InvalidTypeError{
			Type: _dstPtr.Type(),
			Err:  errors.New("not a pointer"),
		})
	}
	dst := _dstPtr.Elem()

	switch dst.Kind() {
	case reflect.Uint8, reflect.Int8:
		if err := binutil.NeedNBytes(dat, sizeof8); err != nil {
			return 0, err
		}
		val := reflect.ValueOf(dat[0])
		dst.Set(val.Convert(dst.Type()))
		return sizeof8, nil
	case reflect.Uint16, reflect.Int16:
		if err := binutil.NeedNBytes(dat, sizeof16); err != nil {
			return 0, err
		}
		val := reflect.ValueOf(binary.LittleEndian.Uint16(dat[:sizeof16]))
		dst.Set(val.Convert(dst.Type()))
		return sizeof16, nil
	case reflect.Uint32, reflect.Int32:
		if err := binutil.NeedNBytes(dat, sizeof32); err != nil {
			return 0, err
		}
		val := reflect.ValueOf(binary.LittleEndian.Uint32(dat[:sizeof32]))
		dst.Set(val.Convert(dst.Type()))
		return sizeof32, nil
	case reflect.Uint64, reflect.Int64:
		if err := binutil.NeedNBytes(dat, sizeof64); err != nil {
			return 0, err
		}
		val := reflect.ValueOf(binary.LittleEndian.Uint64(dat[:sizeof64]))
		dst.Set(val.Convert(dst.Type()))
		return sizeof64, nil
	case reflect.Ptr:
		elemPtr := reflect.New(dst.Type().Elem())
		n, err := Unmarshal(dat, elemPtr.Interface())
		dst.Set(elemPtr.Convert(dst.Type()))
		return n, err
	case reflect.Array:
		var n int
		for i := 0; i < dst.Len(); i++ {
			_n, err := Unmarshal(dat[n:], dst.Index(i).Addr().Interface())
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
			Type: _dstPtr.Type(),
			Err: fmt.Errorf("does not implement binfmt.Unmarshaler and kind=%v is not a supported statically-sized kind",
				dst.Kind()),
		})
	}
}
