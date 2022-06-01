package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

const (
	DEV_STAT_WRITE_ERRS = iota
	DEV_STAT_READ_ERRS
	DEV_STAT_FLUSH_ERRS
	DEV_STAT_CORRUPTION_ERRS
	DEV_STAT_GENERATION_ERRS
	DEV_STAT_VALUES_MAX
)

type DevStats struct { // PERSISTENT_ITEM=249
	Values [DEV_STAT_VALUES_MAX]int64 `bin:"off=0, siz=40"`
	binstruct.End `bin:"off=40"`
}
