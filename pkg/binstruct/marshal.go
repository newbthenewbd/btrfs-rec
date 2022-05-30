package binstruct

import (
	"encoding"
	"fmt"
	"reflect"
)

type Marshaler = encoding.BinaryMarshaler

func Marshal(obj any) ([]byte, error) {
	if mar, ok := obj.(Marshaler); ok {
		return mar.MarshalBinary()
	}
	return MarshalWithoutInterface(obj)
}

func MarshalWithoutInterface(obj any) ([]byte, error) {
	val := reflect.ValueOf(obj)
	switch val.Kind() {
	case reflect.Uint8:
		return val.Convert(u8Type).Interface().(Marshaler).MarshalBinary()
	case reflect.Int8:
		return val.Convert(i8Type).Interface().(Marshaler).MarshalBinary()
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
		panic(fmt.Errorf("type=%v does not implement binfmt.Marshaler and kind=%v is not a supported statically-sized kind",
			val.Type(), val.Kind()))
	}
}
