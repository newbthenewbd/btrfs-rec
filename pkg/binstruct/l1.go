package binstruct

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

type Marshaler interface {
	BinarySize() int64
	MarshalBinary() []byte
}

type Unmarshaler interface {
	Marshaler
	UnmarshalBinary([]byte)
}

type handler interface {
	Unmarshal(dat []byte) interface{}
	Marshal(val interface{}) []byte
	Size() int64
}

type extHandler struct {
	typ reflect.Type
}

func (_ extHandler) Marshal(val interface{}) []byte {
	return val.(Marshaler).MarshalBinary()
}
func (e extHandler) Unmarshal(dat []byte) interface{} {
	valPtr := reflect.New(e.typ).Interface().(Unmarshaler)
	valPtr.UnmarshalBinary(dat)
	return reflect.ValueOf(valPtr).Elem().Interface()
}
func (e extHandler) Size() int64 {
	val := reflect.New(e.typ).Elem().Interface().(Marshaler)
	return val.BinarySize()
}

type primitive struct {
	unmarshal func(dat []byte) interface{}
	marshal   func(val interface{}) []byte
	size      int64
}

func (p primitive) Unmarshal(dat []byte) interface{} { return p.unmarshal(dat) }
func (p primitive) Marshal(val interface{}) []byte   { return p.marshal(val) }
func (p primitive) Size() int64                      { return p.size }

var _ handler = primitive{}

func convert[T any](in interface{}) T {
	var dstTyp T
	return reflect.ValueOf(in).Convert(reflect.TypeOf(dstTyp)).Interface().(T)
}

func genHandler(typ reflect.Type) (handler, error) {
	_, marOK := reflect.New(typ).Elem().Interface().(Marshaler)
	_, unmarOK := reflect.New(typ).Interface().(Unmarshaler)
	if marOK && unmarOK {
		return extHandler{
			typ: typ,
		}, nil
	}
	switch typ.Kind() {
	case reflect.Invalid: // invalid
		return nil, fmt.Errorf("unsupported kind: %s: %v", typ.Kind(), typ)
	case reflect.Bool, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128: // I don't wanna
		return nil, fmt.Errorf("unsupported kind: %s: %v", typ.Kind(), typ)
	case reflect.Int, reflect.Uint, reflect.Uintptr: // platform specific
		return nil, fmt.Errorf("unsupported kind: %s: %v", typ.Kind(), typ)
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.UnsafePointer: // runtime
		return nil, fmt.Errorf("unsupported kind: %s: %v", typ.Kind(), typ)
	case reflect.Map, reflect.Slice, reflect.String: // dynamic size
		return nil, fmt.Errorf("unsupported kind: %s: %v", typ.Kind(), typ)

	// uint ////////////////////////////////////////////////////////////////
	case reflect.Uint8:
		return primitive{
			unmarshal: func(dat []byte) interface{} { return dat[0] },
			marshal:   func(val interface{}) []byte { return []byte{convert[uint8](val)} },
			size:      1,
		}, nil
	case reflect.Uint16:
		return primitive{
			unmarshal: func(dat []byte) interface{} { return binary.LittleEndian.Uint16(dat) },
			marshal: func(val interface{}) []byte {
				var buf [2]byte
				binary.LittleEndian.PutUint16(buf[:], convert[uint16](val))
				return buf[:]
			},
			size: 2,
		}, nil
	case reflect.Uint32:
		return primitive{
			unmarshal: func(dat []byte) interface{} { return binary.LittleEndian.Uint32(dat) },
			marshal: func(val interface{}) []byte {
				var buf [4]byte
				binary.LittleEndian.PutUint32(buf[:], convert[uint32](val))
				return buf[:]
			},
			size: 4,
		}, nil
	case reflect.Uint64:
		return primitive{
			unmarshal: func(dat []byte) interface{} { return binary.LittleEndian.Uint64(dat) },
			marshal: func(val interface{}) []byte {
				var buf [8]byte
				binary.LittleEndian.PutUint64(buf[:], convert[uint64](val))
				return buf[:]
			},
			size: 8,
		}, nil

	// int /////////////////////////////////////////////////////////////////
	case reflect.Int8:
		return primitive{
			unmarshal: func(dat []byte) interface{} { return int8(dat[0]) },
			marshal:   func(val interface{}) []byte { return []byte{uint8(convert[int8](val))} },
			size:      1,
		}, nil
	case reflect.Int16:
		return primitive{
			unmarshal: func(dat []byte) interface{} { return int16(binary.LittleEndian.Uint16(dat)) },
			marshal: func(val interface{}) []byte {
				var buf [2]byte
				binary.LittleEndian.PutUint16(buf[:], uint16(convert[int16](val)))
				return buf[:]
			},
			size: 2,
		}, nil
	case reflect.Int32:
		return primitive{
			unmarshal: func(dat []byte) interface{} { return int32(binary.LittleEndian.Uint32(dat)) },
			marshal: func(val interface{}) []byte {
				var buf [4]byte
				binary.LittleEndian.PutUint32(buf[:], uint32(convert[int32](val)))
				return buf[:]
			},
			size: 4,
		}, nil
	case reflect.Int64:
		return primitive{
			unmarshal: func(dat []byte) interface{} { return int64(binary.LittleEndian.Uint64(dat)) },
			marshal: func(val interface{}) []byte {
				var buf [8]byte
				binary.LittleEndian.PutUint64(buf[:], uint64(convert[int64](val)))
				return buf[:]
			},
			size: 8,
		}, nil

	// composite ///////////////////////////////////////////////////////////

	case reflect.Ptr:
		inner, err := getHandler(typ.Elem())
		if err != nil {
			return nil, fmt.Errorf("%v: %w", typ, err)
		}
		return primitive{
			unmarshal: func(dat []byte) interface{} {
				return reflect.ValueOf(inner.Unmarshal(dat)).Addr().Interface()
			},
			marshal: func(val interface{}) []byte {
				return inner.Marshal(reflect.ValueOf(val).Elem().Interface())
			},
			size: inner.Size(),
		}, nil
	case reflect.Array:
		inner, err := getHandler(typ.Elem())
		if err != nil {
			return nil, fmt.Errorf("%v: %w", typ, err)
		}
		return primitive{
			unmarshal: func(dat []byte) interface{} {
				val := reflect.New(typ).Elem()
				for i := 0; i < typ.Len(); i++ {
					fieldVal := inner.Unmarshal(dat[i*int(inner.Size()):])
					val.Index(i).Set(reflect.ValueOf(fieldVal).Convert(typ.Elem()))
				}
				return val.Interface()
			},
			marshal: func(val interface{}) []byte {
				_val := reflect.ValueOf(val)
				var ret []byte
				for i := 0; i < typ.Len(); i++ {
					ret = append(ret, inner.Marshal(_val.Index(i).Interface())...)
				}
				return ret
			},
			size: inner.Size() * int64(typ.Len()),
		}, nil
	case reflect.Struct:
		return genStructHandler(typ)

	// end /////////////////////////////////////////////////////////////////
	default:
		panic(fmt.Errorf("unknown kind: %v: %v", typ.Kind(), typ))
	}
}
