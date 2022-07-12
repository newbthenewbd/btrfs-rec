// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package binstruct

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct/binutil"
)

type End struct{}

var endType = reflect.TypeOf(End{})

type tag struct {
	skip bool

	off int
	siz int
}

func parseStructTag(str string) (tag, error) {
	var ret tag
	for _, part := range strings.Split(str, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part == "-" {
			return tag{skip: true}, nil
		}
		keyval := strings.SplitN(part, "=", 2)
		if len(keyval) != 2 {
			return tag{}, fmt.Errorf("option is not a key=value pair: %q", part)
		}
		key := keyval[0]
		val := keyval[1]
		switch key {
		case "off":
			vint, err := strconv.ParseInt(val, 0, 0)
			if err != nil {
				return tag{}, err
			}
			ret.off = int(vint)
		case "siz":
			vint, err := strconv.ParseInt(val, 0, 0)
			if err != nil {
				return tag{}, err
			}
			ret.siz = int(vint)
		default:
			return tag{}, fmt.Errorf("unrecognized option %q", key)
		}
	}
	return ret, nil
}

type structHandler struct {
	name   string
	Size   int
	fields []structField
}

type structField struct {
	name string
	tag
}

func (sh structHandler) Unmarshal(dat []byte, dst reflect.Value) (int, error) {
	if err := binutil.NeedNBytes(dat, sh.Size); err != nil {
		return 0, fmt.Errorf("struct %q %w", sh.name, err)
	}
	var n int
	for i, field := range sh.fields {
		if field.skip {
			continue
		}
		_n, err := Unmarshal(dat[n:], dst.Field(i).Addr().Interface())
		if err != nil {
			if _n >= 0 {
				n += _n
			}
			return n, fmt.Errorf("struct %q field %v %q: %w",
				sh.name, i, field.name, err)
		}
		if _n != field.siz {
			return n, fmt.Errorf("struct %q field %v %q: consumed %v bytes but should have consumed %v bytes",
				sh.name, i, field.name, _n, field.siz)
		}
		n += _n
	}
	return n, nil
}

func (sh structHandler) Marshal(val reflect.Value) ([]byte, error) {
	ret := make([]byte, 0, sh.Size)
	for i, field := range sh.fields {
		if field.skip {
			continue
		}
		bs, err := Marshal(val.Field(i).Interface())
		ret = append(ret, bs...)
		if err != nil {
			return ret, fmt.Errorf("struct %q field %v %q: %w",
				sh.name, i, field.name, err)
		}
	}
	return ret, nil
}

func genStructHandler(structInfo reflect.Type) (structHandler, error) {
	var ret structHandler

	ret.name = structInfo.String()

	var curOffset, endOffset int
	for i := 0; i < structInfo.NumField(); i++ {
		var fieldInfo reflect.StructField = structInfo.Field(i)

		if fieldInfo.Anonymous && fieldInfo.Type != endType {
			err := fmt.Errorf("binstruct does not support embedded fields")
			return ret, fmt.Errorf("struct %q field %v %q: %w",
				ret.name, i, fieldInfo.Name, err)
		}

		fieldTag, err := parseStructTag(fieldInfo.Tag.Get("bin"))
		if err != nil {
			return ret, fmt.Errorf("struct %q field %v %q: %w",
				ret.name, i, fieldInfo.Name, err)
		}
		if fieldTag.skip {
			ret.fields = append(ret.fields, structField{
				tag:  fieldTag,
				name: fieldInfo.Name,
			})
			continue
		}

		if fieldTag.off != curOffset {
			err := fmt.Errorf("tag says off=%#x but curOffset=%#x", fieldTag.off, curOffset)
			return ret, fmt.Errorf("struct %q field %v %q: %w",
				ret.name, i, fieldInfo.Name, err)
		}
		if fieldInfo.Type == endType {
			endOffset = curOffset
		}

		fieldSize, err := staticSize(fieldInfo.Type)
		if err != nil {
			return ret, fmt.Errorf("struct %q field %v %q: %w",
				ret.name, i, fieldInfo.Name, err)
		}

		if fieldTag.siz != fieldSize {
			err := fmt.Errorf("tag says siz=%#x but StaticSize(typ)=%#x", fieldTag.siz, fieldSize)
			return ret, fmt.Errorf("struct %q field %v %q: %w",
				ret.name, i, fieldInfo.Name, err)
		}
		curOffset += fieldTag.siz

		ret.fields = append(ret.fields, structField{
			name: fieldInfo.Name,
			tag:  fieldTag,
		})
	}
	ret.Size = curOffset

	if ret.Size != endOffset {
		return ret, fmt.Errorf("struct %q: .Size=%v but endOffset=%v",
			ret.name, ret.Size, endOffset)
	}

	return ret, nil
}

var structCache = make(map[reflect.Type]structHandler)

func getStructHandler(typ reflect.Type) structHandler {
	h, ok := structCache[typ]
	if ok {
		return h
	}

	h, err := genStructHandler(typ)
	if err != nil {
		panic(&InvalidTypeError{
			Type: typ,
			Err:  err,
		})
	}
	structCache[typ] = h
	return h
}
