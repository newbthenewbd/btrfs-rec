package main

import (
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

func main() {
	if err := Main(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "%s: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

func Main(imgfilename string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	fh, err := os.Open(imgfilename)
	if err != nil {
		return err
	}
	img := &Img{
		File: fh,
	}
	defer func() {
		maybeSetErr(img.Close())
	}()

	superblocks, err := img.Superblocks()
	if err != nil {
		return err
	}

	spew := spew.NewDefaultConfig()
	spew.DisablePointerAddresses = true

	spew.Dump(superblocks[0].data)
	sum, err := superblocks[0].data.CalculateChecksum()
	if err != nil {
		return err
	}
	fmt.Printf("calculated sum: %x\n", sum)

	return nil
}

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
	data T
}

func (r *Ref[T]) Read() error {
	size, err := binstruct.Size(r.data)
	if err != nil {
		return err
	}
	buf := make([]byte, size)
	if _, err := r.img.ReadAt(buf, r.addr); err != nil {
		return err
	}
	return binstruct.Unmarshal(buf, &r.data)
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
