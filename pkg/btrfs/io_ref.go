package btrfs

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type Ref[T any] struct {
	dev  *Device
	addr int64
	Data T
}

func (r *Ref[T]) Read() error {
	size, err := binstruct.Size(r.Data)
	if err != nil {
		return err
	}
	buf := make([]byte, size)
	if _, err := r.dev.ReadAt(buf, r.addr); err != nil {
		return err
	}
	return binstruct.Unmarshal(buf, &r.Data)
}
