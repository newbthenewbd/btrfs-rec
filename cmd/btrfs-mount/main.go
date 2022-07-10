package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	"github.com/sirupsen/logrus"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsmisc"
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

func tryAbs(rel string) string {
	abs, err := filepath.Abs(rel)
	if err != nil {
		return rel
	}
	return abs
}

func Main(ctx context.Context, mountpoint string, imgfilenames ...string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	fs, err := btrfsmisc.Open(os.O_RDONLY, imgfilenames...)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fs.Close())
	}()

	rootSubvol := &Subvolume{
		Subvolume: btrfs.Subvolume{
			FS:     fs,
			TreeID: btrfs.FS_TREE_OBJECTID,
		},
		DeviceName: tryAbs(imgfilenames[0]),
		Mountpoint: mountpoint,
	}
	return rootSubvol.Run(ctx)
}
