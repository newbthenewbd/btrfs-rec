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

	fs, sb, err := pass0(imgfilenames...)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fs.Close())
	}()

	foundNodes, err := pass1(fs, sb)
	if err != nil {
		return err
	}

	pass2(fs, foundNodes)

	return nil
}
