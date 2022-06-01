package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

type Type = internal.ItemType

type Item interface {
	isItem()
}
