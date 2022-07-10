// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"context"
	"sync/atomic"

	"github.com/datawire/dlib/dcontext"
	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	"github.com/jacobsa/fuse"
)

func Mount(ctx context.Context, mountpoint string, server fuse.Server, cfg *fuse.MountConfig) error {
	grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{
		// Allow mountHandle.Join() returning to cause the
		// "unmount" goroutine to quit.
		ShutdownOnNonError: true,
	})
	mounted := uint32(1)
	grp.Go("unmount", func(ctx context.Context) error {
		<-ctx.Done()
		var err error
		var gotNil bool
		// Keep retrying, because the FS might be busy.
		for atomic.LoadUint32(&mounted) != 0 {
			if _err := fuse.Unmount(mountpoint); _err == nil {
				gotNil = true
			} else if !gotNil {
				err = _err
			}
		}
		if gotNil {
			return nil
		}
		return err
	})
	grp.Go("mount", func(ctx context.Context) error {
		defer atomic.StoreUint32(&mounted, 0)

		cfg.OpContext = ctx
		cfg.ErrorLogger = dlog.StdLogger(ctx, dlog.LogLevelError)
		cfg.DebugLogger = dlog.StdLogger(ctx, dlog.LogLevelDebug)

		mountHandle, err := fuse.Mount(mountpoint, server, cfg)
		if err != nil {
			return err
		}
		dlog.Infof(ctx, "mounted %q", mountpoint)
		return mountHandle.Join(dcontext.HardContext(ctx))
	})
	return grp.Wait()
}
