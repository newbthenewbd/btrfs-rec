// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

type Optional[T any] struct {
	OK  bool
	Val T
}
