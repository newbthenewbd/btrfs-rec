// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package binstruct

import (
	"fmt"
	"reflect"
)

type InvalidTypeError struct {
	Type reflect.Type
	Err  error
}

func (e *InvalidTypeError) Error() string {
	return fmt.Sprintf("%v: %v", e.Type, e.Err)
}
func (e *InvalidTypeError) Unwrap() error { return e.Err }

type UnmarshalError struct {
	Type   reflect.Type
	Method string
	Err    error
}

func (e *UnmarshalError) Error() string {
	if e.Method == "" {
		return fmt.Sprintf("%v: %v", e.Type, e.Err)
	}
	return fmt.Sprintf("(%v).%v: %v", e.Type, e.Method, e.Err)
}
func (e *UnmarshalError) Unwrap() error { return e.Err }

type MarshalError struct {
	Type   reflect.Type
	Method string
	Err    error
}

func (e *MarshalError) Error() string {
	if e.Method == "" {
		return fmt.Sprintf("%v: %v", e.Type, e.Err)
	}
	return fmt.Sprintf("(%v).%v: %v", e.Type, e.Method, e.Err)
}
func (e *MarshalError) Unwrap() error { return e.Err }
