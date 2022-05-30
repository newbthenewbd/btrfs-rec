package binstruct

import (
	"fmt"
	"reflect"
)

type Unmarshaler interface {
	UnmarshalBinary([]byte) (int, error)
}

func Unmarshal(dat []byte, dstPtr any) (int, error) {
	if unmar, ok := dstPtr.(Unmarshaler); ok {
		return unmar.UnmarshalBinary(dat)
	}
	_dstPtr := reflect.ValueOf(dstPtr)
	if _dstPtr.Kind() != reflect.Ptr {
		return 0, fmt.Errorf("not a pointer: %v", _dstPtr.Type())
	}
	dst := _dstPtr.Elem()

	switch dst.Kind() {
	case reflect.Ptr:
		elemPtr := reflect.New(dst.Type().Elem()).Interface()
		n, err := Unmarshal(dat, elemPtr)
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
		// TODO
	default:
		panic(fmt.Errorf("type=%v does not implement binfmt.Unmarshaler and kind=%v is not a supported statically-sized kind",
			val.Type(), val.Kind()))
	}
}
