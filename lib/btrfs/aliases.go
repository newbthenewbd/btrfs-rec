// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/internal"
	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

type (
	// (u)int64 types

	Generation = internal.Generation
	ObjID      = internal.ObjID

	// complex types

	Key  = internal.Key
	Time = internal.Time
	UUID = util.UUID
)
