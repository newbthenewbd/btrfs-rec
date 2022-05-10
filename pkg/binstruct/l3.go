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

func Unmarshal(dat []byte, dst interface{}) error {
	_dst := reflect.ValueOf(dst)
	if _dst.Kind() != reflect.Ptr {
		return fmt.Errorf("not a pointer: %v", _dst.Type())
	}
	handler, err := getHandler(_dst.Type().Elem())
	if err != nil {
		return err
	}
	if int64(len(dat)) < handler.Size() {
		return fmt.Errorf("need at least %d bytes of data, only have %d",
			handler.Size(), len(dat))
	}
	val := handler.Unmarshal(dat[:handler.Size()])
	_dst.Elem().Set(reflect.ValueOf(val))
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
