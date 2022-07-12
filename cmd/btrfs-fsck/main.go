// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"context"
	"fmt"
	"os"
)

func main() {
	if err := Main(context.Background(), os.Args[1:]...); err != nil {
		fmt.Fprintf(os.Stderr, "%v: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

func Main(ctx context.Context, imgfilenames ...string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	fs, sb, err := pass0(ctx, imgfilenames...)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fs.Close())
	}()

	foundNodes, err := pass1(ctx, fs, sb)
	if err != nil {
		return err
	}

	pass2(fs, foundNodes)

	return nil
}
