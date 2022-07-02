package main

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/btrfsmisc"
	"lukeshu.com/btrfs-tools/pkg/util"
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

	fs, err := btrfsmisc.Open(os.O_RDONLY, imgfilenames...)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fs.Close())
	}()

	sb, err := fs.Superblock()
	if err != nil {
		return err
	}

	fsTreeRoot, err := fs.TreeLookup(sb.Data.RootTree, btrfs.Key{
		ObjectID: btrfs.FS_TREE_OBJECTID,
		ItemType: btrfsitem.ROOT_ITEM_KEY,
		Offset:   0,
	})
	if err != nil {
		return fmt.Errorf("look up FS_TREE: %w", err)
	}
	fsTreeRootBody := fsTreeRoot.Body.(btrfsitem.Root)
	fsTree := fsTreeRootBody.ByteNr

	return printDir(fs, fsTree, "", "/", fsTreeRootBody.RootDirID, fsTreeRootBody.Inode)
}

func printDir(fs *btrfs.FS, fsTree btrfsvol.LogicalAddr, prefix, dirName string, dirInodeNum btrfs.ObjID, dirInode btrfsitem.Inode) error {
	fmt.Printf("%s[%s\tino=%d\tuid=%d\tgid=%d\tsize=%d] %s\n",
		prefix,
		dirInode.Mode, dirInodeNum, dirInode.UID, dirInode.GID, dirInode.Size,
		dirName)
	items, err := fs.TreeSearchAll(fsTree, func(key btrfs.Key) int {
		return util.CmpUint(dirInodeNum, key.ObjectID)
	})
	if err != nil {
		return fmt.Errorf("read directory %q: %w", dirName, err)
	}
	for _, item := range items {
		switch item.Head.Key.ItemType {
		case btrfsitem.INODE_ITEM_KEY:
			// TODO
		case btrfsitem.INODE_REF_KEY:
			// TODO
		case btrfsitem.DIR_ITEM_KEY:
			// skip?
		case btrfsitem.DIR_INDEX_KEY:
			for _, entry := range item.Body.(btrfsitem.DirList) {
				fmt.Println(string(entry.Name))
			}
		case btrfsitem.XATTR_ITEM_KEY:
		default:
			panic(fmt.Errorf("TODO: handle item type %v", item.Head.Key.ItemType))
		}
	}
	return nil
}
