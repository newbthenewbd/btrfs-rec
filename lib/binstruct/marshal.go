// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package binstruct

import (
	"encoding"
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
	case reflect.Uint8, reflect.Int8, reflect.Uint16, reflect.Int16, reflect.Uint32, reflect.Int32, reflect.Uint64, reflect.Int64:
		typ := intKind2Type[val.Kind()]
		dat, err := val.Convert(typ).Interface().(Marshaler).MarshalBinary()
		if err != nil {
			err = &UnmarshalError{
				Type:   typ,
				Method: "MarshalBinary",
				Err:    err,
			}
		}
		return dat, err
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
