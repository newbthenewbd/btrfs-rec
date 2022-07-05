package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/datawire/dlib/dcontext"
	"github.com/datawire/dlib/dlog"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
)

type Subvolume struct {
	FS         *btrfs.FS
	DeviceName string
	Mountpoint string
	TreeID     btrfs.ObjID

	fuseutil.NotImplementedFileSystem

	rootOnce sync.Once
	rootVal  btrfsitem.Root
	rootErr  error
}

func (sv *Subvolume) Run(ctx context.Context) error {
	mount, err := fuse.Mount(
		sv.Mountpoint,
		fuseutil.NewFileSystemServer(sv),
		&fuse.MountConfig{
			OpContext:   ctx,
			ErrorLogger: dlog.StdLogger(ctx, dlog.LogLevelError),
			DebugLogger: dlog.StdLogger(ctx, dlog.LogLevelDebug),

			FSName:  sv.DeviceName,
			Subtype: "btrfs",

			ReadOnly: true,
		})
	if err != nil {
		return err
	}
	return mount.Join(dcontext.HardContext(ctx))
}

func (sv *Subvolume) initRoot() {
	sv.rootOnce.Do(func() {
		sb, err := sv.FS.Superblock()
		if err != nil {
			sv.rootErr = err
			return
		}

		root, err := sv.FS.TreeLookup(sb.Data.RootTree, btrfs.Key{
			ObjectID: sv.TreeID,
			ItemType: btrfsitem.ROOT_ITEM_KEY,
			Offset:   0,
		})
		if err != nil {
			sv.rootErr = err
			return
		}

		rootBody, ok := root.Body.(btrfsitem.Root)
		if !ok {
			sv.rootErr = fmt.Errorf("FS_TREE_ ROOT_ITEM has malformed body")
			return
		}

		sv.rootVal = rootBody
	})
}

func (sv *Subvolume) getRootInode() (btrfs.ObjID, error) {
	sv.initRoot()
	return sv.rootVal.RootDirID, sv.rootErr
}

func (sv *Subvolume) getFSTree() (btrfsvol.LogicalAddr, error) {
	sv.initRoot()
	return sv.rootVal.ByteNr, sv.rootErr
}

func (sv *Subvolume) StatFS(_ context.Context, op *fuseops.StatFSOp) error {
	// See linux.git/fs/btrfs/super.c:btrfs_statfs()
	sb, err := sv.FS.Superblock()
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

// func (sv *Subvolume) LookUpInode(_ context.Context, op *fuseops.LookUpInodeOp) error               {}

func (sv *Subvolume) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) error {
	if op.Inode == fuseops.RootInodeID {
		inode, err := sv.getRootInode()
		if err != nil {
			return err
		}
		op.Inode = fuseops.InodeID(inode)
	}

	tree, err := sv.getFSTree()
	if err != nil {
		return err
	}

	item, err := sv.FS.TreeLookup(tree, btrfs.Key{
		ObjectID: btrfs.ObjID(op.Inode),
		ItemType: btrfsitem.INODE_ITEM_KEY,
		Offset:   0,
	})
	if err != nil {
		return err
	}

	itemBody, ok := item.Body.(btrfsitem.Inode)
	if !ok {
		return fmt.Errorf("malformed inode")
	}

	op.Attributes = fuseops.InodeAttributes{
		Size:  uint64(itemBody.Size),
		Nlink: uint32(itemBody.NLink),
		Mode:  uint32(itemBody.Mode),
		//RDev: itemBody.Rdev, // jacobsa/fuse doesn't expose rdev
		Atime: itemBody.ATime.ToStd(),
		Mtime: itemBody.MTime.ToStd(),
		Ctime: itemBody.CTime.ToStd(),
		//Crtime: itemBody.OTime,
		Uid: uint32(itemBody.UID),
		Gid: uint32(itemBody.GID),
	}
	return nil
}

// func (sv *Subvolume) ForgetInode(_ context.Context, op *fuseops.ForgetInodeOp) error               {}
// func (sv *Subvolume) BatchForget(_ context.Context, op *fuseops.BatchForgetOp) error               {}
// func (sv *Subvolume) OpenDir(_ context.Context, op *fuseops.OpenDirOp) error                       {}
// func (sv *Subvolume) ReadDir(_ context.Context, op *fuseops.ReadDirOp) error                       {}
// func (sv *Subvolume) ReleaseDirHandle(_ context.Context, op *fuseops.ReleaseDirHandleOp) error     {}
// func (sv *Subvolume) OpenFile(_ context.Context, op *fuseops.OpenFileOp) error                     {}
// func (sv *Subvolume) ReadFile(_ context.Context, op *fuseops.ReadFileOp) error                     {}
// func (sv *Subvolume) ReleaseFileHandle(_ context.Context, op *fuseops.ReleaseFileHandleOp) error   {}
// func (sv *Subvolume) ReadSymlink(_ context.Context, op *fuseops.ReadSymlinkOp) error               {}
// func (sv *Subvolume) GetXattr(_ context.Context, op *fuseops.GetXattrOp) error                     {}
// func (sv *Subvolume) ListXattr(_ context.Context, op *fuseops.ListXattrOp) error                   {}
// func (sv *Subvolume) Destroy()                                                                {}
