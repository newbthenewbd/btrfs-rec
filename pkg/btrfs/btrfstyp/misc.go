package btrfstyp

import (
	"time"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

type (
	PhysicalAddr int64
	LogicalAddr  int64
	Generation   uint64
)

type Key struct {
	ObjectID      ObjID             `bin:"off=0, siz=8"` // Each tree has its own set of Object IDs.
	ItemType      internal.ItemType `bin:"off=8, siz=1"`
	Offset        uint64            `bin:"off=9, siz=8"` // The meaning depends on the item type.
	binstruct.End `bin:"off=11"`
}

type Time struct {
	Sec           int64  `bin:"off=0, siz=8"` // Number of seconds since 1970-01-01T00:00:00Z.
	NSec          uint32 `bin:"off=8, siz=4"` // Number of nanoseconds since the beginning of the second.
	binstruct.End `bin:"off=c"`
}

func (t Time) ToStd() time.Time {
	return time.Unix(t.Sec, int64(t.NSec))
}
