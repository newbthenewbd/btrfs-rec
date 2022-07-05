package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"syscall"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type dirState struct {
	Dir *dir
}

type subvolumeFUSE struct {
	fuseutil.NotImplementedFileSystem
	lastHandle uint64
	dirHandles util.SyncMap[uint64, *dirState]
}

func (sv *subvolumeFUSE) init() {}

func inodeItemToFUSE(itemBody btrfsitem.Inode) fuseops.InodeAttributes {
	return fuseops.InodeAttributes{
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

func (sv *Subvolume) LookUpInode(_ context.Context, op *fuseops.LookUpInodeOp) error {
	if op.Parent == fuseops.RootInodeID {
		parent, err := sv.getRootInode()
		if err != nil {
			return err
		}
		op.Parent = fuseops.InodeID(parent)
	}

	dir, err := sv.loadDir(btrfs.ObjID(op.Parent))
	if err != nil {
		return err
	}
	entry, ok := dir.ChildrenByName[op.Name]
	if !ok {
		return syscall.ENOENT
	}
	if entry.Location.ItemType != btrfsitem.INODE_ITEM_KEY {
		return fmt.Errorf("child %q is not an inode: %w", op.Name, syscall.ENOSYS)
	}
	inodeItem, err := sv.loadInode(entry.Location.ObjectID)
	if err != nil {
		return err
	}
	op.Entry = fuseops.ChildInodeEntry{
		Child:      fuseops.InodeID(entry.Location.ObjectID),
		Generation: fuseops.GenerationNumber(inodeItem.Sequence),
		Attributes: inodeItemToFUSE(inodeItem),
	}
	return nil
}

func (sv *Subvolume) GetInodeAttributes(_ context.Context, op *fuseops.GetInodeAttributesOp) error {
	if op.Inode == fuseops.RootInodeID {
		inode, err := sv.getRootInode()
		if err != nil {
			return err
		}
		op.Inode = fuseops.InodeID(inode)
	}

	inodeItem, err := sv.loadInode(btrfs.ObjID(op.Inode))
	if err != nil {
		return err
	}

	op.Attributes = inodeItemToFUSE(inodeItem)
	return nil
}

func (sv *Subvolume) OpenDir(_ context.Context, op *fuseops.OpenDirOp) error {
	if op.Inode == fuseops.RootInodeID {
		inode, err := sv.getRootInode()
		if err != nil {
			return err
		}
		op.Inode = fuseops.InodeID(inode)
	}

	dir, err := sv.loadDir(btrfs.ObjID(op.Inode))
	if err != nil {
		return err
	}
	handle := atomic.AddUint64(&sv.lastHandle, 1)
	sv.dirHandles.Store(handle, &dirState{
		Dir: dir,
	})
	op.Handle = fuseops.HandleID(handle)
	return nil
}

func (sv *Subvolume) ReadDir(_ context.Context, op *fuseops.ReadDirOp) error {
	state, ok := sv.dirHandles.Load(uint64(op.Handle))
	if !ok {
		return syscall.EBADF
	}
	indexes := util.SortedMapKeys(state.Dir.ChildrenByIndex)
	origOffset := op.Offset
	for _, index := range indexes {
		if index < uint64(origOffset) {
			continue
		}
		entry := state.Dir.ChildrenByIndex[index]
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], fuseutil.Dirent{
			Offset: fuseops.DirOffset(index + 1),
			Inode:  fuseops.InodeID(entry.Location.ObjectID),
			Name:   string(entry.Name),
			Type: map[btrfsitem.FileType]fuseutil.DirentType{
				btrfsitem.FT_UNKNOWN:  fuseutil.DT_Unknown,
				btrfsitem.FT_REG_FILE: fuseutil.DT_File,
				btrfsitem.FT_DIR:      fuseutil.DT_Directory,
				btrfsitem.FT_CHRDEV:   fuseutil.DT_Char,
				btrfsitem.FT_BLKDEV:   fuseutil.DT_Block,
				btrfsitem.FT_FIFO:     fuseutil.DT_FIFO,
				btrfsitem.FT_SOCK:     fuseutil.DT_Socket,
				btrfsitem.FT_SYMLINK:  fuseutil.DT_Link,
			}[entry.Type],
		})
		if n == 0 {
			break
		}
		op.BytesRead += n
	}
	return nil
}

func (sv *Subvolume) ReleaseDirHandle(_ context.Context, op *fuseops.ReleaseDirHandleOp) error {
	_, ok := sv.dirHandles.LoadAndDelete(uint64(op.Handle))
	if !ok {
		return syscall.EBADF
	}
	return nil
}

func (sv *Subvolume) OpenFile(_ context.Context, op *fuseops.OpenFileOp) error { return syscall.ENOSYS }
func (sv *Subvolume) ReadFile(_ context.Context, op *fuseops.ReadFileOp) error { return syscall.ENOSYS }
func (sv *Subvolume) ReleaseFileHandle(_ context.Context, op *fuseops.ReleaseFileHandleOp) error {
	return syscall.ENOSYS
}

func (sv *Subvolume) ReadSymlink(_ context.Context, op *fuseops.ReadSymlinkOp) error {
	return syscall.ENOSYS
}

func (sv *Subvolume) GetXattr(_ context.Context, op *fuseops.GetXattrOp) error { return syscall.ENOSYS }
func (sv *Subvolume) ListXattr(_ context.Context, op *fuseops.ListXattrOp) error {
	return syscall.ENOSYS
}

func (sv *Subvolume) Destroy() {}
