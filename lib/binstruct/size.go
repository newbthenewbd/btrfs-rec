package binstruct

import (
	"fmt"
	"reflect"
)

type StaticSizer interface {
	BinaryStaticSize() int
}

func StaticSize(obj any) int {
	sz, err := staticSize(reflect.TypeOf(obj))
	if err != nil {
		panic(err)
	}
	return sz
}

var (
	staticSizerType = reflect.TypeOf((*StaticSizer)(nil)).Elem()
	marshalerType   = reflect.TypeOf((*Marshaler)(nil)).Elem()
	unmarshalerType = reflect.TypeOf((*Unmarshaler)(nil)).Elem()
)

func staticSize(typ reflect.Type) (int, error) {
	if typ.Implements(staticSizerType) {
		return reflect.New(typ).Elem().Interface().(StaticSizer).BinaryStaticSize(), nil
	}
	switch typ.Kind() {
	case reflect.Uint8, reflect.Int8:
		return 1, nil
	case reflect.Uint16, reflect.Int16:
		return 2, nil
	case reflect.Uint32, reflect.Int32:
		return 4, nil
	case reflect.Uint64, reflect.Int64:
		return 8, nil
	case reflect.Ptr:
		return staticSize(typ.Elem())
	case reflect.Array:
		elemSize, err := staticSize(typ.Elem())
		if err != nil {
			return 0, err
		}
		return elemSize * typ.Len(), nil
	case reflect.Struct:
		if !(typ.Implements(marshalerType) || typ.Implements(unmarshalerType)) {
			return getStructHandler(typ).Size, nil
		}
		return 0, fmt.Errorf("type=%v (kind=%v) does not implement binfmt.StaticSizer but does implement binfmt.Marshaler or binfmt.Unmarshaler",
			typ, typ.Kind())
	default:
		return 0, fmt.Errorf("type=%v does not implement binfmt.StaticSizer and kind=%v is not a supported statically-sized kind",
			typ, typ.Kind())
	}
}
