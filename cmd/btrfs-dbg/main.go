package main

import (
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
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
	img := &btrfs.Img{
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

	sum, err := superblocks[0].Data.CalculateChecksum()
	if err != nil {
		return err
	}
	fmt.Printf("superblock checksum: %x\n", sum)
	spew.Dump(superblocks[0].Data)

	syschunks, err := superblocks[0].Data.ParseSysChunkArray()
	if err != nil {
		return err
	}
	spew.Dump(syschunks)

	if err := img.ScanForNodes(superblocks[0].Data); err != nil {
		return err
	}

	return nil
}
