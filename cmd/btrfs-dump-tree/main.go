package main

import (
	"fmt"
	"os"
	"strings"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
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
		for _, item := range node.Body {
			fmt.Printf("\t%s block %d gen %d\n",
				fmtKey(item.Data.Key),
				item.Data.BlockPtr,
				item.Data.Generation)
		}
		for _, item := range node.Body {
			printTree(fs, item.Data.BlockPtr)
		}
	case *btrfs.LeafNode:
		for i, item := range node.Body {
			fmt.Printf("\titem %d %s itemoff %d itemsize %d\n",
				i,
				fmtKey(item.Data.Key),
				item.Data.DataOffset,
				item.Data.DataSize)
			switch item.Data.Key.ItemType {
			case btrfs.BTRFS_UNTYPED_KEY:
				// TODO
			case btrfs.BTRFS_INODE_ITEM_KEY:
				// TODO(!)
			case btrfs.BTRFS_INODE_REF_KEY:
				dat := item.Data.Data.Data
				for len(dat) > 0 {
					var ref btrfs.InodeRefItem
					if err := binstruct.Unmarshal(dat, &ref); err != nil {
						fmt.Printf("error: %v\n", err)
						return
					}
					dat = dat[0xA:]
					ref.Name = dat[:ref.NameLen]
					dat = dat[ref.NameLen:]

					fmt.Printf("\t\tindex %d namelen %d name: %s\n",
						ref.Index, ref.NameLen, ref.Name)
				}
			case btrfs.BTRFS_INODE_EXTREF_KEY:
				// TODO
			case btrfs.BTRFS_DIR_ITEM_KEY, btrfs.BTRFS_DIR_INDEX_KEY, btrfs.BTRFS_XATTR_ITEM_KEY:
				// TODO
			case btrfs.BTRFS_DIR_LOG_INDEX_KEY, btrfs.BTRFS_DIR_LOG_ITEM_KEY:
				// TODO
			case btrfs.BTRFS_ORPHAN_ITEM_KEY:
				fmt.Printf("\t\torphan item\n")
			case btrfs.BTRFS_ROOT_ITEM_KEY:
				var obj btrfs.RootItem
				if err := binstruct.Unmarshal(item.Data.Data.Data, &obj); err != nil {
					fmt.Printf("error: %v\n", err)
					return
				}
				fmt.Printf("\t\tgeneration %d root_dirid %d bytenr %d byte_limit %d bytes_used %d\n",
					obj.Generation, obj.RootDirID, obj.ByteNr, obj.ByteLimit, obj.BytesUsed)
				fmt.Printf("\t\tlast_snapshot %d flags %s refs %d\n",
					obj.LastSnapshot, obj.Flags, obj.Refs)
				fmt.Printf("\t\tdrop_progress %s drop_level %d\n",
					fmtKey(obj.DropProgress), obj.DropLevel)
				fmt.Printf("\t\tlevel %d generation_v2 %d\n",
					obj.Level, obj.GenerationV2)
				if obj.Generation == obj.GenerationV2 {
					fmt.Printf("\t\tuuid %s\n", obj.UUID)
					fmt.Printf("\t\tparent_uuid %s\n", obj.ParentUUID)
					fmt.Printf("\t\treceived_uuid %s\n", obj.ReceivedUUID)
					fmt.Printf("\t\tctransid %d otransid %d stransid %d rtransid %d\n",
						obj.CTransID, obj.OTransID, obj.STransID, obj.RTransID)
					fmt.Printf("\t\tctime %s\n", fmtTime(obj.CTime))
					fmt.Printf("\t\totime %s\n", fmtTime(obj.OTime))
					fmt.Printf("\t\tstime %s\n", fmtTime(obj.STime))
					fmt.Printf("\t\trtime %s\n", fmtTime(obj.RTime))
				}
			case btrfs.BTRFS_ROOT_REF_KEY:
				// TODO
			case btrfs.BTRFS_ROOT_BACKREF_KEY:
				// TODO
			case btrfs.BTRFS_EXTENT_ITEM_KEY:
				// TODO
			case btrfs.BTRFS_METADATA_ITEM_KEY:
				// TODO
			case btrfs.BTRFS_TREE_BLOCK_REF_KEY:
				fmt.Printf("\t\ttree block backref\n")
			case btrfs.BTRFS_SHARED_BLOCK_REF_KEY:
				fmt.Printf("\t\tshared block backref\n")
			case btrfs.BTRFS_EXTENT_DATA_REF_KEY:
				// TODO
			case btrfs.BTRFS_SHARED_DATA_REF_KEY:
				// TODO
			case btrfs.BTRFS_EXTENT_REF_V0_KEY:
				fmt.Printf("\t\textent ref v0 (deprecated)\n")
			case btrfs.BTRFS_CSUM_ITEM_KEY:
				fmt.Printf("\t\tcsum item\n")
			case btrfs.BTRFS_EXTENT_CSUM_KEY:
				// TODO
			case btrfs.BTRFS_EXTENT_DATA_KEY:
				// TODO
			case btrfs.BTRFS_BLOCK_GROUP_ITEM_KEY:
				// TODO
			case btrfs.BTRFS_FREE_SPACE_INFO_KEY:
				// TODO
			case btrfs.BTRFS_FREE_SPACE_EXTENT_KEY:
				fmt.Printf("\t\tfree space extent\n")
			case btrfs.BTRFS_FREE_SPACE_BITMAP_KEY:
				fmt.Printf("\t\tfree space bitmap\n")
			case btrfs.BTRFS_CHUNK_ITEM_KEY:
				// TODO(!)
			case btrfs.BTRFS_DEV_ITEM_KEY:
				// TODO
			case btrfs.BTRFS_DEV_EXTENT_KEY:
				// TODO
			case btrfs.BTRFS_QGROUP_STATUS_KEY:
				// TODO
			case btrfs.BTRFS_QGROUP_RELATION_KEY, btrfs.BTRFS_QGROUP_INFO_KEY:
				// TODO
			case btrfs.BTRFS_QGROUP_LIMIT_KEY:
				// TODO
			case btrfs.BTRFS_UUID_KEY_SUBVOL, btrfs.BTRFS_UUID_KEY_RECEIVED_SUBVOL:
				// TODO
			case btrfs.BTRFS_STRING_ITEM_KEY:
				// TODO
			case btrfs.BTRFS_PERSISTENT_ITEM_KEY:
				// TODO
			case btrfs.BTRFS_TEMPORARY_ITEM_KEY:
				// TODO
			}
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

	fmt.Printf("checksum stored %x\n", node.GetNodeHeader().Data.Checksum)
	fmt.Printf("checksum calced %v\n", "TODO")

	fmt.Printf("fs uuid %s\n", node.GetNodeHeader().Data.MetadataUUID)
	fmt.Printf("chunk uuid %s\n", node.GetNodeHeader().Data.ChunkTreeUUID)
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

func fmtTime(t btrfs.Time) string {
	return fmt.Sprintf("%d.%d (%s)",
		t.Sec, t.NSec, t.ToStd().Format("2006-01-02 15:04:05"))
}
