package btrfs

import (
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
	"lukeshu.com/btrfs-tools/pkg/util"
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
