// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree

import (
	iofs "io/fs"
)

// For both ErrNoItem and ErrNoTree, `errors.Is(err,
// io/fs.ErrNotExist)` returns true.
var (
	ErrNoItem = errNotExist("item")
	ErrNoTree = errNotExist("tree")
)

func errNotExist(thing string) error {
	return &notExistError{thing}
}

type notExistError struct {
	thing string
}

func (e *notExistError) Error() string {
	return e.thing + " does not exist"
}

func (*notExistError) Is(target error) bool {
	return target == iofs.ErrNotExist
}
