//go:build old
// +build old

package binstruct

import (
	"fmt"
	"reflect"
)

var handlerCache = make(map[reflect.Type]handler)

func getHandler(typ reflect.Type) (handler, error) {
	h, ok := handlerCache[typ]
	if ok {
		return h, nil
	}

	h, err := genHandler(typ)
	if err != nil {
		return nil, err
	}
	handlerCache[typ] = h
	return h, nil
}

func Unmarshal(dat []byte, dstPtr interface{}) error {
	_dstPtr := reflect.ValueOf(dstPtr)
	if _dstPtr.Kind() != reflect.Ptr {
		return fmt.Errorf("not a pointer: %v", _dstPtr.Type())
	}

	dst := _dstPtr.Elem()
	handler, err := getHandler(dst.Type())
	if err != nil {
		return err
	}
	if int64(len(dat)) < handler.Size() {
		return fmt.Errorf("need at least %d bytes of data, only have %d",
			handler.Size(), len(dat))
	}
	val := handler.Unmarshal(dat[:handler.Size()])
	dst.Set(reflect.ValueOf(val).Convert(dst.Type()))
	return nil
}

func Marshal(val interface{}) ([]byte, error) {
	handler, err := getHandler(reflect.TypeOf(val))
	if err != nil {
		return nil, err
	}
	bs := handler.Marshal(val)
	if int64(len(bs)) != handler.Size() {
		return nil, fmt.Errorf("got %d bytes but expected %d bytes",
			len(bs), handler.Size())
	}
	return bs, nil
}

func Size(val interface{}) (int64, error) {
	handler, err := getHandler(reflect.TypeOf(val))
	if err != nil {
		return 0, err
	}
	return handler.Size(), nil
}
