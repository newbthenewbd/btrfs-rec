package util

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type File[A ~int64] interface {
	Name() string
	Size() (A, error)
	ReadAt(p []byte, off A) (n int, err error)
}

type Ref[A ~int64, T any] struct {
	File File[A]
	Addr A
	Data T
}

func (r *Ref[A, T]) Read() error {
	size := binstruct.StaticSize(r.Data)
	buf := make([]byte, size)
	if _, err := r.File.ReadAt(buf, r.Addr); err != nil {
		return err
	}
	if _, err := binstruct.Unmarshal(buf, &r.Data); err != nil {
		return err
	}
	return nil
}
