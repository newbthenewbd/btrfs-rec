package main

import (
	"fmt"
	"os"
	"strings"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/util"
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
		if err := printTree(fs, superblock.Data.RootTree); err != nil {
			return err
		}
	}
	if superblock.Data.ChunkTree != 0 {
		fmt.Printf("chunk tree\n")
		if err := printTree(fs, superblock.Data.ChunkTree); err != nil {
			return err
		}
	}
	if superblock.Data.LogTree != 0 {
		fmt.Printf("log root tree\n")
		if err := printTree(fs, superblock.Data.LogTree); err != nil {
			return err
		}
	}
	if superblock.Data.BlockGroupRoot != 0 {
		fmt.Printf("block group tree\n")
		if err := printTree(fs, superblock.Data.BlockGroupRoot); err != nil {
			return err
		}
	}
	if err := fs.WalkTree(superblock.Data.RootTree, func(key btrfs.Key, dat []byte) error {
		if key.ItemType != btrfsitem.ROOT_ITEM_KEY {
			return nil
		}
		var obj btrfsitem.Root
		n, err := binstruct.Unmarshal(dat, &obj)
		if err != nil {
			return err
		}
		if n != len(dat) {
			return fmt.Errorf("left over data: got %d bytes but only consumed %d", len(dat), n)
		}
		return printTree(fs, obj.ByteNr)
	}); err != nil {
		return err
	}

	return nil
}

// printTree mimics btrfs-progs kernel-shared/print-tree.c:btrfs_print_tree() and  kernel-shared/print-tree.c:btrfs_print_leaf()
func printTree(fs *btrfs.FS, root btrfs.LogicalAddr) error {
	nodeRef, err := fs.ReadNode(root)
	if err != nil {
		return err
	}
	node := nodeRef.Data
	printHeaderInfo(node)
	if node.Head.Level > 0 { // internal
		for _, item := range node.BodyInternal {
			fmt.Printf("\t%s block %d gen %d\n",
				fmtKey(item.Key),
				item.BlockPtr,
				item.Generation)
		}
		for _, item := range node.BodyInternal {
			if err := printTree(fs, item.BlockPtr); err != nil {
				return err
			}
		}
	} else { // leaf
		for i, item := range node.BodyLeaf {
			fmt.Printf("\titem %d %s itemoff %d itemsize %d\n",
				i,
				fmtKey(item.Head.Key),
				item.Head.DataOffset,
				item.Head.DataSize)
			switch item.Head.Key.ItemType {
			//case btrfsitem.UNTYPED_KEY:
			//	// TODO
			//case btrfsitem.INODE_ITEM_KEY:
			//	// TODO(!)
			case btrfsitem.INODE_REF_KEY:
				dat := item.Body
				for len(dat) > 0 {
					var ref btrfsitem.InodeRef
					n, err := binstruct.Unmarshal(dat, &ref)
					if err != nil {
						return err
					}
					fmt.Printf("\t\tindex %d namelen %d name: %s\n",
						ref.Index, ref.NameLen, ref.Name)
					dat = dat[n:]
				}
			//case btrfsitem.INODE_EXTREF_KEY:
			//	// TODO
			//case btrfsitem.DIR_ITEM_KEY, btrfsitem.DIR_INDEX_KEY, btrfsitem.XATTR_ITEM_KEY:
			//	// TODO
			//case btrfsitem.DIR_LOG_INDEX_KEY, btrfsitem.DIR_LOG_ITEM_KEY:
			//	// TODO
			case btrfsitem.ORPHAN_ITEM_KEY:
				fmt.Printf("\t\torphan item\n")
			case btrfsitem.ROOT_ITEM_KEY:
				var obj btrfsitem.Root
				n, err := binstruct.Unmarshal(item.Body, &obj)
				if err != nil {
					return err
				}
				if n != len(item.Body) {
					return fmt.Errorf("left over data: got %d bytes but only consumed %d", len(item.Body), n)
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
			//case btrfsitem.ROOT_REF_KEY:
			//	// TODO
			//case btrfsitem.ROOT_BACKREF_KEY:
			//	// TODO
			//case btrfsitem.EXTENT_ITEM_KEY:
			//	// TODO
			//case btrfsitem.METADATA_ITEM_KEY:
			//	// TODO
			//case btrfsitem.TREE_BLOCK_REF_KEY:
			//	fmt.Printf("\t\ttree block backref\n")
			//case btrfsitem.SHARED_BLOCK_REF_KEY:
			//	fmt.Printf("\t\tshared block backref\n")
			//case btrfsitem.EXTENT_DATA_REF_KEY:
			//	// TODO
			//case btrfsitem.SHARED_DATA_REF_KEY:
			//	// TODO
			//case btrfsitem.EXTENT_REF_V0_KEY:
			//	fmt.Printf("\t\textent ref v0 (deprecated)\n")
			//case btrfsitem.CSUM_ITEM_KEY:
			//	fmt.Printf("\t\tcsum item\n")
			//case btrfsitem.EXTENT_CSUM_KEY:
			//	// TODO
			//case btrfsitem.EXTENT_DATA_KEY:
			//	// TODO
			//case btrfsitem.BLOCK_GROUP_ITEM_KEY:
			//	// TODO
			//case btrfsitem.FREE_SPACE_INFO_KEY:
			//	// TODO
			//case btrfsitem.FREE_SPACE_EXTENT_KEY:
			//	fmt.Printf("\t\tfree space extent\n")
			//case btrfsitem.FREE_SPACE_BITMAP_KEY:
			//	fmt.Printf("\t\tfree space bitmap\n")
			//case btrfsitem.CHUNK_ITEM_KEY:
			//	// TODO(!)
			//case btrfsitem.DEV_ITEM_KEY:
			//	// TODO
			//case btrfsitem.DEV_EXTENT_KEY:
			//	// TODO
			//case btrfsitem.QGROUP_STATUS_KEY:
			//	// TODO
			//case btrfsitem.QGROUP_RELATION_KEY, btrfsitem.QGROUP_INFO_KEY:
			//	// TODO
			//case btrfsitem.QGROUP_LIMIT_KEY:
			//	// TODO
			case btrfsitem.UUID_SUBVOL_KEY, btrfsitem.UUID_RECEIVED_SUBVOL_KEY:
				// TODO
				//case btrfsitem.STRING_ITEM_KEY:
				//	// TODO
				//case btrfsitem.PERSISTENT_ITEM_KEY:
				//	// TODO
				//case btrfsitem.TEMPORARY_ITEM_KEY:
				//	// TODO
			}
		}
	}
	return nil
}

// printHeaderInfo mimics btrfs-progs kernel-shared/print-tree.c:print_header_info()
func printHeaderInfo(node btrfs.Node) {
	var typename string
	if node.Head.Level > 0 { // internal node
		typename = "node"
		fmt.Printf("node %d level %d items %d free space %d",
			node.Head.Addr,
			node.Head.Level,
			node.Head.NumItems,
			node.MaxItems()-node.Head.NumItems)
	} else { // leaf node
		typename = "leaf"
		fmt.Printf("leaf %d items %d free space %d",
			node.Head.Addr,
			node.Head.NumItems,
			node.LeafFreeSpace())
	}
	fmt.Printf(" generation %d owner %v\n",
		node.Head.Generation,
		node.Head.Owner)

	fmt.Printf("%s %d flags %s backref revision %d\n",
		typename,
		node.Head.Addr,
		node.Head.Flags,
		node.Head.BackrefRev)

	fmt.Printf("checksum stored %x\n", node.Head.Checksum)
	fmt.Printf("checksum calced %v\n", "TODO")

	fmt.Printf("fs uuid %s\n", node.Head.MetadataUUID)
	fmt.Printf("chunk uuid %s\n", node.Head.ChunkTreeUUID)
}

// mimics print-tree.c:btrfs_print_key()
func fmtKey(key btrfs.Key) string {
	var out strings.Builder
	fmt.Fprintf(&out, "key (%s %v", key.ObjectID.Format(key.ItemType), key.ItemType)
	switch key.ItemType {
	case btrfsitem.QGROUP_RELATION_KEY: //TODO, btrfsitem.QGROUP_INFO_KEY, btrfsitem.QGROUP_LIMIT_KEY:
		panic("not implemented")
	case btrfsitem.UUID_SUBVOL_KEY, btrfsitem.UUID_RECEIVED_SUBVOL_KEY:
		fmt.Fprintf(&out, " 0x%016x)", key.Offset)
	case btrfsitem.ROOT_ITEM_KEY:
		fmt.Fprintf(&out, " %v)", btrfs.ObjID(key.Offset))
	default:
		if key.Offset == util.MaxUint64pp-1 {
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
