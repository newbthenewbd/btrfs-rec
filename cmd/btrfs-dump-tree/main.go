package main

import (
	"fmt"
	"os"
	"strings"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
)

func main() {
	if err := Main(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "%s: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

const version = "5.17"

func Main(imgfilename string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	fh, err := os.Open(imgfilename)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fh.Close())
	}()
	fs := &btrfs.FS{
		Devices: []*btrfs.Device{
			{
				File: fh,
			},
		},
	}

	if err := fs.Init(); err != nil {
		return err
	}

	superblock, err := fs.Superblock()
	if err != nil {
		return err
	}

	fmt.Printf("btrfs-progs v%s \n", version)
	if superblock.Data.RootTree != 0 {
		fmt.Printf("root tree\n")
		printTree(fs, superblock.Data.RootTree)
	}
	if superblock.Data.ChunkTree != 0 {
		fmt.Printf("chunk tree\n")
		printTree(fs, superblock.Data.ChunkTree)
	}
	if superblock.Data.LogTree != 0 {
		fmt.Printf("log root tree\n")
		printTree(fs, superblock.Data.LogTree)
	}
	if superblock.Data.BlockGroupRoot != 0 {
		fmt.Printf("block group tree\n")
		printTree(fs, superblock.Data.BlockGroupRoot)
	}

	return nil
}

// printTree mimics btrfs-progs kernel-shared/print-tree.c:btrfs_print_tree() and  kernel-shared/print-tree.c:btrfs_print_leaf()
func printTree(fs *btrfs.FS, root btrfs.LogicalAddr) {
	node, err := fs.ReadNode(root)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
	printHeaderInfo(node)
	switch node := node.(type) {
	case *btrfs.InternalNode:
		// TODO
	case *btrfs.LeafNode:
		for i, item := range node.Body {
			fmt.Printf("\titem %d %s itemoff %d itemsize %d\n",
				i,
				fmtKey(item.Data.Key),
				item.Data.DataOffset,
				item.Data.DataSize)
		}
	}
}

// printHeaderInfo mimics btrfs-progs kernel-shared/print-tree.c:print_header_info()
func printHeaderInfo(node btrfs.Node) {
	var typename string
	switch node := node.(type) {
	case *btrfs.InternalNode:
		typename = "node"
		fmt.Printf("node %d level %d items %d free space %d",
			node.Header.Addr,
			node.Header.Data.Level,
			node.Header.Data.NumItems,
			node.Header.Data.MaxItems-node.Header.Data.NumItems)
	case *btrfs.LeafNode:
		typename = "leaf"
		fmt.Printf("leaf %d items %d free space %d",
			node.Header.Addr,
			node.Header.Data.NumItems,
			node.FreeSpace())
	}
	fmt.Printf(" generation %d owner %v\n",
		node.GetNodeHeader().Data.Generation,
		node.GetNodeHeader().Data.Owner)

	fmt.Printf("%s %d flags %s backref revision %d\n",
		typename,
		node.GetNodeHeader().Addr,
		node.GetNodeHeader().Data.Flags,
		node.GetNodeHeader().Data.BackrefRev)
}

// mimics print-tree.c:btrfs_print_key()
func fmtKey(key btrfs.Key) string {
	var out strings.Builder
	fmt.Fprintf(&out, "key (%s %v", key.ObjectID.Format(key.ItemType), key.ItemType)
	switch key.ItemType {
	case btrfs.BTRFS_QGROUP_RELATION_KEY, btrfs.BTRFS_QGROUP_INFO_KEY, btrfs.BTRFS_QGROUP_LIMIT_KEY:
		panic("not implemented")
	case btrfs.BTRFS_UUID_KEY_SUBVOL, btrfs.BTRFS_UUID_KEY_RECEIVED_SUBVOL:
		fmt.Fprintf(&out, " 0x%016x)", key.Offset)
	case btrfs.BTRFS_ROOT_ITEM_KEY:
		fmt.Fprintf(&out, " %v)", btrfs.ObjID(key.Offset))
	default:
		if key.Offset == btrfs.MaxUint64pp-1 {
			fmt.Fprintf(&out, " -1)")
		} else {
			fmt.Fprintf(&out, " %d)", key.Offset)
		}
	}
	return out.String()
}
