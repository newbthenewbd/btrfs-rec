package btrfs

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type Img struct {
	*os.File
}

func (img *Img) Size() (int64, error) {
	fi, err := img.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

type Ref[T any] struct {
	img  *Img
	addr int64
	Data T
}

func (r *Ref[T]) Read() error {
	size, err := binstruct.Size(r.Data)
	if err != nil {
		return err
	}
	buf := make([]byte, size)
	if _, err := r.img.ReadAt(buf, r.addr); err != nil {
		return err
	}
	return binstruct.Unmarshal(buf, &r.Data)
}

func (img *Img) Superblocks() ([]Ref[Superblock], error) {
	const superblockSize = 0x1000

	var superblockAddrs = []int64{
		0x00_0001_0000, // 64KiB
		0x00_0400_0000, // 64MiB
		0x40_0000_0000, // 256GiB
	}

	sz, err := img.Size()
	if err != nil {
		return nil, err
	}

	var ret []Ref[Superblock]
	for i, addr := range superblockAddrs {
		if addr+superblockSize <= sz {
			superblock := Ref[Superblock]{
				img:  img,
				addr: addr,
			}
			if err := superblock.Read(); err != nil {
				return nil, fmt.Errorf("superblock %d: %w", i, err)
			}
			ret = append(ret, superblock)
		}
	}
	if len(ret) == 0 {
		return nil, fmt.Errorf("no superblocks")
	}
	return ret, nil
}
