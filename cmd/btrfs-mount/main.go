// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	"github.com/sirupsen/logrus"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
)

func main() {
	ctx := context.Background()
	logger := logrus.New()
	//logger.SetLevel(logrus.TraceLevel)
	ctx = dlog.WithLogger(ctx, dlog.WrapLogrus(logger))

	grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{
		EnableSignalHandling: true,
	})
	grp.Go("main", func(ctx context.Context) error {
		return Main(ctx, os.Args[1], os.Args[2:]...)
	})
	if err := grp.Wait(); err != nil {
		fmt.Fprintf(os.Stderr, "%v: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

func Main(ctx context.Context, mountpoint string, imgfilenames ...string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	fs, err := btrfsutil.Open(os.O_RDONLY, imgfilenames...)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fs.Close())
	}()

	return btrfsinspect.MountRO(ctx, fs, mountpoint)
}
