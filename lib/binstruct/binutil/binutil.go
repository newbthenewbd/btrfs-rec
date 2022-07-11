// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

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
