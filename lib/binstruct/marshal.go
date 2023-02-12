// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package binstruct

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"reflect"
)

type Marshaler = encoding.BinaryMarshaler

func Marshal(obj any) ([]byte, error) {
	if mar, ok := obj.(Marshaler); ok {
		dat, err := mar.MarshalBinary()
		if err != nil {
			err = &UnmarshalError{
				Type:   reflect.TypeOf(obj),
				Method: "MarshalBinary",
				Err:    err,
			}
		}
		return dat, err
	}
	return MarshalWithoutInterface(obj)
}

func MarshalWithoutInterface(obj any) ([]byte, error) {
	val := reflect.ValueOf(obj)
	switch val.Kind() {
	case reflect.Uint8:
		var buf [sizeof8]byte
		buf[0] = byte(val.Uint())
		return buf[:], nil
	case reflect.Int8:
		var buf [sizeof8]byte
		buf[0] = byte(val.Int())
		return buf[:], nil
	case reflect.Uint16:
		var buf [sizeof16]byte
		binary.LittleEndian.PutUint16(buf[:], uint16(val.Uint()))
		return buf[:], nil
	case reflect.Int16:
		var buf [sizeof16]byte
		binary.LittleEndian.PutUint16(buf[:], uint16(val.Int()))
		return buf[:], nil
	case reflect.Uint32:
		var buf [sizeof32]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(val.Uint()))
		return buf[:], nil
	case reflect.Int32:
		var buf [sizeof32]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(val.Int()))
		return buf[:], nil
	case reflect.Uint64:
		var buf [sizeof64]byte
		binary.LittleEndian.PutUint64(buf[:], val.Uint())
		return buf[:], nil
	case reflect.Int64:
		var buf [sizeof64]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(val.Int()))
		return buf[:], nil
	case reflect.Ptr:
		return Marshal(val.Elem().Interface())
	case reflect.Array:
		var ret []byte
		for i := 0; i < val.Len(); i++ {
			bs, err := Marshal(val.Index(i).Interface())
			ret = append(ret, bs...)
			if err != nil {
				return ret, err
			}
		}
		return ret, nil
	case reflect.Struct:
		return getStructHandler(val.Type()).Marshal(val)
	default:
		panic(&InvalidTypeError{
			Type: val.Type(),
			Err: fmt.Errorf("does not implement binfmt.Marshaler and kind=%v is not a supported statically-sized kind",
				val.Kind()),
		})
	}
}
