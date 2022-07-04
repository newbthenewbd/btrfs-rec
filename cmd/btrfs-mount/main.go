package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/datawire/dlib/dcontext"
	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/sirupsen/logrus"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
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

	fsAdapter := &FSAdapter{
		fs: fs,
	}

	devname, err := filepath.Abs(imgfilenames[0])
	if err != nil {
		devname = imgfilenames[0]
	}
	mount, err := fuse.Mount(
		mountpoint,
		fuseutil.NewFileSystemServer(fsAdapter),
		&fuse.MountConfig{
			OpContext:   ctx,
			ErrorLogger: dlog.StdLogger(ctx, dlog.LogLevelError),
			DebugLogger: dlog.StdLogger(ctx, dlog.LogLevelDebug),

			FSName:  devname,
			Subtype: "btrfs",

			ReadOnly: true,
		})
	if err != nil {
		return err
	}

	mounted := uint32(1)
	grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{
		ShutdownOnNonError: true,
	})
	grp.Go("send-unmount", func(ctx context.Context) error {
		<-ctx.Done()
		if atomic.LoadUint32(&mounted) == 0 {
			return nil
		}
		return fuse.Unmount(os.Args[1])
	})
	grp.Go("recv-unmount", func(ctx context.Context) error {
		ret := mount.Join(dcontext.HardContext(ctx))
		atomic.StoreUint32(&mounted, 0)
		return ret
	})
	return grp.Wait()
}

type FSAdapter struct {
	fuseutil.NotImplementedFileSystem

	fs *btrfs.FS

	rootInodeOnce sync.Once
	rootInodeVal  btrfs.ObjID
	rootInodeErr  error

	inode2treeMu sync.Mutex
	inode2tree   map[btrfs.ObjID]btrfsvol.LogicalAddr
}

func (a *FSAdapter) getRootInode() (btrfs.ObjID, error) {
	a.rootInodeOnce.Do(func() {
		a.rootInodeVal, a.rootInodeErr = func() (btrfs.ObjID, error) {
			sb, err := a.fs.Superblock()
			if err != nil {
				return 0, err
			}

			root, err := fs.TreeLookup(sb.Data.RootTree, btrfs.Key{
				ObjectID: btrfs.FS_TREE_OBJECTID,
				ItemType: btrfsitem.ROOT_ITEM_KEY,
				Offset:   0,
			})
			if err != nil {
				return 0, err
			}
			rootBody, ok := root.Body.(btrfsitem.Root)
			if !ok {
				return 0, fmt.Errorf("FS_TREE_ ROOT_ITEM has malformed body")
			}
			return rootBody.RootDirID, nil
		}()
	})
	return a.rootInodeVal, a.rootInodeErr
}

func (a *FSAdapter) StatFS(_ context.Context, op *fuseops.StatFSOp) error {
	// See linux.git/fs/btrfs/super.c:btrfs_statfs()
	sb, err := a.fs.Superblock()
	if err != nil {
		return err
	}

	op.IoSize = sb.Data.SectorSize
	op.BlockSize = sb.Data.SectorSize
	op.Blocks = sb.Data.TotalBytes / uint64(sb.Data.SectorSize) // TODO: adjust for RAID type
	//op.BlocksFree = TODO

	// btrfs doesn't have a fixed number of inodes
	op.Inodes = 0
	op.InodesFree = 0

	// jacobsa/fuse doesn't expose namelen, instead hard-coding it
	// to 255.  Which is fine by us, because that's what it is for
	// btrfs.

	return nil
}

// func (a *FSAdapter) LookUpInode(_ context.Context, op *fuseops.LookUpInodeOp) error               {}

func (a *FSAdapter) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) error {
	sb, err := a.fs.Superblock()
	if err != nil {
		return err
	}

	if op.Inode == fuseops.RootInodeID {
		io.Inode, err = a.getRootInode()
		if err != nil {
			return err
		}
	}

}

// func (a *FSAdapter) ForgetInode(_ context.Context, op *fuseops.ForgetInodeOp) error               {}
// func (a *FSAdapter) BatchForget(_ context.Context, op *fuseops.BatchForgetOp) error               {}
// func (a *FSAdapter) OpenDir(_ context.Context, op *fuseops.OpenDirOp) error                       {}
// func (a *FSAdapter) ReadDir(_ context.Context, op *fuseops.ReadDirOp) error                       {}
// func (a *FSAdapter) ReleaseDirHandle(_ context.Context, op *fuseops.ReleaseDirHandleOp) error     {}
// func (a *FSAdapter) OpenFile(_ context.Context, op *fuseops.OpenFileOp) error                     {}
// func (a *FSAdapter) ReadFile(_ context.Context, op *fuseops.ReadFileOp) error                     {}
// func (a *FSAdapter) ReleaseFileHandle(_ context.Context, op *fuseops.ReleaseFileHandleOp) error   {}
// func (a *FSAdapter) ReadSymlink(_ context.Context, op *fuseops.ReadSymlinkOp) error               {}
// func (a *FSAdapter) GetXattr(_ context.Context, op *fuseops.GetXattrOp) error                     {}
// func (a *FSAdapter) ListXattr(_ context.Context, op *fuseops.ListXattrOp) error                   {}
// func (a *FSAdapter) Destroy()                                                                {}
