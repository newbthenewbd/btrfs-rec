// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package textui

// Tunable annotates a value as something that might want to be tuned
// as the program gets optimized.
//
// TODO(lukeshu): Have Tunable be runtime-configurable.
func Tunable[T any](x T) T {
	return x
}
