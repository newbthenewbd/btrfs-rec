package binstruct

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type End struct{}

type structHandler struct {
	typ    reflect.Type
	fields []structField
	size   int64
}

type structField struct {
	typ reflect.Type
	tag
	handler
	name string
}

func (sh structHandler) Unmarshal(dat []byte) interface{} {
	val := reflect.New(sh.typ).Elem()
	for i, field := range sh.fields {
		if field.skip {
			continue
		}
		fieldVal := field.Unmarshal(dat[field.off:])
		val.Field(i).Set(reflect.ValueOf(fieldVal).Convert(field.typ))
	}
	return val.Interface()
}
func (sh structHandler) Marshal(_val interface{}) []byte {
	val := reflect.ValueOf(_val)
	ret := make([]byte, 0, sh.size)
	for i, field := range sh.fields {
		if field.skip {
			continue
		}
		if int64(len(ret)) != field.off {
			panic(fmt.Errorf("field %d %q: len(ret)=0x%x but field.off=0x%x", i, field.name, len(ret), field.off))
		}
		ret = append(ret, field.Marshal(val.Field(i).Interface())...)
	}
	return ret
}
func (sh structHandler) Size() int64 {
	return sh.size
}

var _ handler = structHandler{}

func genStructHandler(structInfo reflect.Type) (handler, error) {
	ret := structHandler{
		typ: structInfo,
	}

	var curOffset, endOffset int64
	for i := 0; i < structInfo.NumField(); i++ {
		var fieldInfo reflect.StructField = structInfo.Field(i)

		fieldTag, err := parseStructTag(fieldInfo.Tag.Get("bin"))
		if err != nil {
			return nil, fmt.Errorf("%v: field %q: %w",
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
			return nil, fmt.Errorf("%v: field %q: %w",
				structInfo, fieldInfo.Name, err)
		}
		if fieldInfo.Type == reflect.TypeOf(End{}) {
			endOffset = curOffset
		}

		fieldHandler, err := getHandler(fieldInfo.Type)
		if err != nil {
			return nil, fmt.Errorf("%v: field %q: %w",
				structInfo, fieldInfo.Name, err)
		}

		if fieldTag.siz != fieldHandler.Size() {
			err := fmt.Errorf("tag says siz=0x%x but handler.Size()=0x%x", fieldTag.siz, fieldHandler.Size())
			return nil, fmt.Errorf("%v: field %q: %w",
				structInfo, fieldInfo.Name, err)
		}
		curOffset += fieldTag.siz

		ret.fields = append(ret.fields, structField{
			typ:     fieldInfo.Type,
			tag:     fieldTag,
			handler: fieldHandler,
			name:    fieldInfo.Name,
		})
	}
	ret.size = curOffset

	if ret.size != endOffset {
		return nil, fmt.Errorf("%v: .size=%v but endOffset=%v",
			structInfo, ret.size, endOffset)
	}

	return ret, nil
}

type tag struct {
	skip bool

	off  int64
	siz  int64
	desc string
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
			vint, err := strconv.ParseInt(val, 16, 64)
			if err != nil {
				return tag{}, err
			}
			ret.off = vint
		case "siz":
			vint, err := strconv.ParseInt(val, 16, 64)
			if err != nil {
				return tag{}, err
			}
			ret.siz = vint
		case "desc":
			ret.desc = val
		}
	}
	return ret, nil
}
