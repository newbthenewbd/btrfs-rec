// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package binutil provides utilities for implementing the interfaces
// consumed by binstruct.
package binutil

import (
	"fmt"
)

func NeedNBytes(dat []byte, n int) error {
	if len(dat) < n {
		return fmt.Errorf("need at least %v bytes, only have %v", n, len(dat))
	}
	return nil
}
