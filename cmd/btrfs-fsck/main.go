package main

import (
	"fmt"
	"os"
)

func main() {
	if err := Main(os.Args[1:]...); err != nil {
		fmt.Fprintf(os.Stderr, "%v: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

func Main(imgfilenames ...string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	var imgfiles []*os.File
	for _, imgfilename := range imgfilenames {
		fh, err := os.OpenFile(imgfilename, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		defer func() {
			maybeSetErr(fh.Close())
		}()
		imgfiles = append(imgfiles, fh)
	}

	fs, sb, err := pass0(imgfiles...)
	if err != nil {
		return err
	}

	foundNodes, err := pass1(fs, sb)
	if err != nil {
		return err
	}

	pass2(fs, foundNodes)

	return nil
}
