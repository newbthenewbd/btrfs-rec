package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type InodeRef struct { // INODE_REF=12
	Index         int64 `bin:"off=0x0, siz=0x8"`
	NameLen       int16 `bin:"off=0x8, siz=0x2"`
	binstruct.End `bin:"off=0xa"`
	Name          []byte `bin:"-"`
}
