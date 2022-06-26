package btrfs

import (
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

type (
	// (u)int64 types

	Generation   = internal.Generation
	ObjID        = internal.ObjID
	LogicalAddr  = internal.LogicalAddr
	PhysicalAddr = internal.PhysicalAddr
	AddrDelta    = internal.AddrDelta

	// complex types

	Key  = internal.Key
	Time = internal.Time
	UUID = internal.UUID
)
