package binstruct

import (
	"fmt"
	"reflect"
)

type StaticSizer interface {
	BinaryStaticSize() int
}

func StaticSize(obj any) int {
	return staticSize(reflect.TypeOf(obj))
}

var staticSizerType = reflect.TypeOf((*StaticSizer)(nil)).Elem()

func staticSize(typ reflect.Type) int {
	if typ.Implements(staticSizerType) {
		return reflect.New(typ).Elem().Interface().(StaticSizer).BinaryStaticSize()
	}
	if szer, ok := obj.(StaticSizer); ok {
		return szer.BinaryStaticSize()
	}
	switch typ.Kind() {
	case reflect.Ptr:
		return StaticSize(typ.Elem())
	case reflect.Array:
		return StaticSize(typ.Elem()) * typ.Len()
	case reflect.Struct:
		// TODO
	default:
		panic(fmt.Errorf("type=%v does not implement binfmt.StaticSizer and kind=%v is not a supported statically-sized kind",
			typ, typ.Kind()))
	}
}
