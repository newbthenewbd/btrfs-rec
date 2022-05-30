package binstruct

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
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
	Size   int
	fields []structField
}

type structField struct {
	name string
	tag
}

func (sh structHandler) Unmarshal(dat []byte, dst reflect.Value) (int, error) {
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
			return n, fmt.Errorf("field %d %q: %w",
				i, field.name, err)
		}
		if _n != field.siz {
			return n, fmt.Errorf("field %d %q: consumed %d bytes but should have consumed %d bytes",
				i, field.name, _n, field.siz)
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
			return ret, fmt.Errorf("field %d %q: %w",
				i, field.name, err)
		}
	}
	return ret, nil
}

func genStructHandler(structInfo reflect.Type) (structHandler, error) {
	var ret structHandler

	var curOffset, endOffset int
	for i := 0; i < structInfo.NumField(); i++ {
		var fieldInfo reflect.StructField = structInfo.Field(i)

		fieldTag, err := parseStructTag(fieldInfo.Tag.Get("bin"))
		if err != nil {
			return ret, fmt.Errorf("%v: field %q: %w",
				structInfo, fieldInfo.Name, err)
		}
		if fieldTag.skip {
			ret.fields = append(ret.fields, structField{
				tag:  fieldTag,
				name: fieldInfo.Name,
			})
			continue
		}

		if fieldTag.off != curOffset {
			err := fmt.Errorf("tag says off=0x%x but curOffset=0x%x", fieldTag.off, curOffset)
			return ret, fmt.Errorf("%v: field %q: %w",
				structInfo, fieldInfo.Name, err)
		}
		if fieldInfo.Type == endType {
			endOffset = curOffset
		}

		fieldSize, err := staticSize(fieldInfo.Type)
		if err != nil {
			return ret, fmt.Errorf("%v: field %q: %w",
				structInfo, fieldInfo.Name, err)
		}

		if fieldTag.siz != fieldSize {
			err := fmt.Errorf("tag says siz=0x%x but StaticSize(typ)=0x%x", fieldTag.siz, fieldSize)
			return ret, fmt.Errorf("%v: field %q: %w",
				structInfo, fieldInfo.Name, err)
		}
		curOffset += fieldTag.siz

		ret.fields = append(ret.fields, structField{
			name: fieldInfo.Name,
			tag:  fieldTag,
		})
	}
	ret.Size = curOffset

	if ret.Size != endOffset {
		return ret, fmt.Errorf("%v: .Size=%v but endOffset=%v",
			structInfo, ret.Size, endOffset)
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
		panic(err)
	}
	structCache[typ] = h
	return h
}
