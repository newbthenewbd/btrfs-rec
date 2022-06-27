package main

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
)

func main() {
	if err := Main(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "%v: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

func Main(imgfilename string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	fh, err := os.OpenFile(imgfilename, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fh.Close())
	}()

	fs := new(btrfs.FS)
	if err := fs.AddDevice(&btrfs.Device{File: fh}); err != nil {
		return err
	}

	superblock, err := pass0(fs)
	if err != nil {
		return err
	}

	foundNodes, err := pass1(fs, superblock)
	if err != nil {
		return err
	}

	pass2(fs, foundNodes)

	return nil
}
