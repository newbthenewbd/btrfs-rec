package btrfs

import (
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type (
	// (u)int64 types

	Generation   = internal.Generation
	ObjID        = internal.ObjID
	LogicalAddr  = btrfsvol.LogicalAddr
	PhysicalAddr = btrfsvol.PhysicalAddr
	AddrDelta    = btrfsvol.AddrDelta

	// complex types

	Key                   = internal.Key
	Time                  = internal.Time
	UUID                  = util.UUID
	QualifiedPhysicalAddr = btrfsvol.QualifiedPhysicalAddr
)
