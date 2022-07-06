package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	"github.com/jacobsa/fuse"
	"github.com/sirupsen/logrus"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfsmisc"
)

func main() {
	ctx := context.Background()
	logger := logrus.New()
	logger.SetLevel(logrus.TraceLevel)
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

	mounted := uint32(1)
	grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{
		ShutdownOnNonError: true,
	})
	grp.Go("shutdown", func(ctx context.Context) error {
		<-ctx.Done()
		if atomic.LoadUint32(&mounted) == 0 {
			return nil
		}
		return fuse.Unmount(os.Args[1])
	})
	grp.Go("mount", func(ctx context.Context) error {
		defer atomic.StoreUint32(&mounted, 0)
		rootSubvol := &Subvolume{
			Subvolume: btrfs.Subvolume{
				FS:     fs,
				TreeID: btrfs.FS_TREE_OBJECTID,
			},
			DeviceName: tryAbs(imgfilenames[0]),
			Mountpoint: mountpoint,
		}
		return rootSubvol.Run(ctx, false)
	})
	return grp.Wait()
}
