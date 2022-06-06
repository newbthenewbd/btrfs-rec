package util

import (
	"fmt"
	"io"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type File[A ~int64] interface {
	Name() string
	Size() (A, error)
	ReadAt(p []byte, off A) (n int, err error)
	WriteAt(p []byte, off A) (n int, err error)
}

var (
	_ io.WriterAt = File[int64](nil)
	_ io.ReaderAt = File[int64](nil)
)

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
	n, err := binstruct.Unmarshal(buf, &r.Data)
	if err != nil {
		return err
	}
	if n != size {
		return fmt.Errorf("util.Ref[%T].Read: left over data: read %d bytes but only consumed %d",
			r.Data, size, n)
	}
	return nil
}

func (r *Ref[A, T]) Write() error {
	buf, err := binstruct.Marshal(r.Data)
	if err != nil {
		return err
	}
	if _, err = r.File.WriteAt(buf, r.Addr); err != nil {
		return err
	}
	return nil
}
