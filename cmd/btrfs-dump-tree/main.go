package main

import (
	"fmt"
	"os"
	"strings"

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
	if err := fs.WalkTree(superblock.Data.RootTree, btrfs.WalkTreeHandler{
		Item: func(key btrfs.Key, body btrfsitem.Item) error {
			if key.ItemType != btrfsitem.ROOT_ITEM_KEY {
				return nil
			}
			treeName, ok := map[btrfs.ObjID]string{
				btrfs.ROOT_TREE_OBJECTID:        "root",
				btrfs.EXTENT_TREE_OBJECTID:      "extent",
				btrfs.CHUNK_TREE_OBJECTID:       "chunk",
				btrfs.DEV_TREE_OBJECTID:         "device",
				btrfs.FS_TREE_OBJECTID:          "fs",
				btrfs.ROOT_TREE_DIR_OBJECTID:    "directory",
				btrfs.CSUM_TREE_OBJECTID:        "checksum",
				btrfs.ORPHAN_OBJECTID:           "orphan",
				btrfs.TREE_LOG_OBJECTID:         "log",
				btrfs.TREE_LOG_FIXUP_OBJECTID:   "log fixup",
				btrfs.TREE_RELOC_OBJECTID:       "reloc",
				btrfs.DATA_RELOC_TREE_OBJECTID:  "data reloc",
				btrfs.EXTENT_CSUM_OBJECTID:      "extent checksum",
				btrfs.QUOTA_TREE_OBJECTID:       "quota",
				btrfs.UUID_TREE_OBJECTID:        "uuid",
				btrfs.FREE_SPACE_TREE_OBJECTID:  "free space",
				btrfs.MULTIPLE_OBJECTIDS:        "multiple",
				btrfs.BLOCK_GROUP_TREE_OBJECTID: "block group",
			}[key.ObjectID]
			if !ok {
				treeName = "file"
			}
			fmt.Printf("%s tree %s \n", treeName, fmtKey(key))
			return printTree(fs, body.(btrfsitem.Root).ByteNr)
		},
	}); err != nil {
		return err
	}

	return nil
}

// printTree mimics btrfs-progs kernel-shared/print-tree.c:btrfs_print_tree() and  kernel-shared/print-tree.c:btrfs_print_leaf()
func printTree(fs *btrfs.FS, root btrfs.LogicalAddr) error {
	nodeRef, err := fs.ReadNode(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return nil
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
			switch body := item.Body.(type) {
			case btrfsitem.Inode:
				fmt.Printf(""+
					"\t\tgeneration %d transid %d size %d nbytes %d\n"+
					"\t\tblock group %d mode %o links %d uid %d gid %d rdev %d\n"+
					"\t\tsequence %d flags %v\n",
					body.Generation, body.TransID, body.Size, body.NumBytes,
					body.BlockGroup, body.Mode, body.NLink, body.UID, body.GID, body.RDev,
					body.Sequence, body.Flags)
				fmt.Printf("\t\tatime %s\n", fmtTime(body.ATime))
				fmt.Printf("\t\tctime %s\n", fmtTime(body.CTime))
				fmt.Printf("\t\tmtime %s\n", fmtTime(body.MTime))
				fmt.Printf("\t\totime %s\n", fmtTime(body.OTime))
			case btrfsitem.InodeRefList:
				for _, ref := range body {
					fmt.Printf("\t\tindex %d namelen %d name: %s\n",
						ref.Index, ref.NameLen, ref.Name)
				}
			//case btrfsitem.INODE_EXTREF_KEY:
			//	// TODO
			case btrfsitem.DirList:
				for _, dir := range body {
					fmt.Printf("\t\tlocation %s type %v\n",
						fmtKey(dir.Location), dir.Type)
					fmt.Printf("\t\ttransid %d data_len %d name_len %d\n",
						dir.TransID, dir.DataLen, dir.NameLen)
					fmt.Printf("\t\tname: %s\n", dir.Name)
					if len(dir.Data) > 0 {
						fmt.Printf("\t\tdata %s\n", dir.Data)
					}
				}
			//case btrfsitem.DIR_LOG_INDEX_KEY, btrfsitem.DIR_LOG_ITEM_KEY:
			//	// TODO
			case btrfsitem.Root:
				fmt.Printf("\t\tgeneration %d root_dirid %d bytenr %d byte_limit %d bytes_used %d\n",
					body.Generation, body.RootDirID, body.ByteNr, body.ByteLimit, body.BytesUsed)
				fmt.Printf("\t\tlast_snapshot %d flags %s refs %d\n",
					body.LastSnapshot, body.Flags, body.Refs)
				fmt.Printf("\t\tdrop_progress %s drop_level %d\n",
					fmtKey(body.DropProgress), body.DropLevel)
				fmt.Printf("\t\tlevel %d generation_v2 %d\n",
					body.Level, body.GenerationV2)
				if body.Generation == body.GenerationV2 {
					fmt.Printf("\t\tuuid %s\n", body.UUID)
					fmt.Printf("\t\tparent_uuid %s\n", body.ParentUUID)
					fmt.Printf("\t\treceived_uuid %s\n", body.ReceivedUUID)
					fmt.Printf("\t\tctransid %d otransid %d stransid %d rtransid %d\n",
						body.CTransID, body.OTransID, body.STransID, body.RTransID)
					fmt.Printf("\t\tctime %s\n", fmtTime(body.CTime))
					fmt.Printf("\t\totime %s\n", fmtTime(body.OTime))
					fmt.Printf("\t\tstime %s\n", fmtTime(body.STime))
					fmt.Printf("\t\trtime %s\n", fmtTime(body.RTime))
				}
			//case btrfsitem.ROOT_REF_KEY:
			//	// TODO
			//case btrfsitem.ROOT_BACKREF_KEY:
			//	// TODO
			case btrfsitem.Extent:
				fmt.Printf("\t\trefs %d gen %d flags %v\n",
					body.Head.Refs, body.Head.Generation, body.Head.Flags)
				if body.Head.Flags.Has(btrfsitem.EXTENT_FLAG_TREE_BLOCK) {
					fmt.Printf("\t\ttree block %s level %d\n",
						fmtKey(body.Info.Key), body.Info.Level)
				}
				printExtentInlineRefs(body.Refs)
			case btrfsitem.Metadata:
				fmt.Printf("\t\trefs %d gen %d flags %v\n",
					body.Head.Refs, body.Head.Generation, body.Head.Flags)
				fmt.Printf("\t\ttree block skinny level %d\n", item.Head.Key.Offset)
				printExtentInlineRefs(body.Refs)
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
			case btrfsitem.Chunk:
				fmt.Printf("\t\tlength %d owner %d stripe_len %d type %v\n",
					body.Size, body.Owner, body.StripeLen, body.Type)
				fmt.Printf("\t\tio_align %d io_width %d sector_size %d\n",
					body.IOOptimalAlign, body.IOOptimalWidth, body.IOMinSize)
				fmt.Printf("\t\tnum_stripes %d sub_stripes %d\n",
					body.NumStripes, body.SubStripes)
				for i, stripe := range body.Stripes {
					fmt.Printf("\t\t\tstripe %d devid %d offset %d\n",
						i, stripe.DeviceID, stripe.Offset)
					fmt.Printf("\t\t\tdev_uuid %s\n",
						stripe.DeviceUUID)
				}
			case btrfsitem.Dev:
				fmt.Printf(""+
					"\t\tdevid %d total_bytes %d bytes_used %d\n"+
					"\t\tio_align %d io_width %d sector_size %d type %d\n"+
					"\t\tgeneration %d start_offset %d dev_group %d\n"+
					"\t\tseek_speed %d bandwidth %d\n"+
					"\t\tuuid %s\n"+
					"\t\tfsid %s\n",
					body.DeviceID, body.NumBytes, body.NumBytesUsed,
					body.IOOptimalAlign, body.IOOptimalWidth, body.IOMinSize, body.Type,
					body.Generation, body.StartOffset, body.DevGroup,
					body.SeekSpeed, body.Bandwidth,
					body.DevUUID,
					body.FSUUID)
			//case btrfsitem.DEV_EXTENT_KEY:
			//	// TODO
			//case btrfsitem.QGROUP_STATUS_KEY:
			//	// TODO
			//case btrfsitem.QGROUP_RELATION_KEY, btrfsitem.QGROUP_INFO_KEY:
			//	// TODO
			//case btrfsitem.QGROUP_LIMIT_KEY:
			//	// TODO
			//case btrfsitem.UUIDMap:
			//	// TODO
			//case btrfsitem.STRING_ITEM_KEY:
			//	// TODO
			//case btrfsitem.PERSISTENT_ITEM_KEY:
			//	// TODO
			//case btrfsitem.TEMPORARY_ITEM_KEY:
			//	// TODO
			case btrfsitem.Empty:
				switch item.Head.Key.ItemType {
				case btrfsitem.ORPHAN_ITEM_KEY:
					fmt.Printf("\t\torphan item\n")
				default:
					fmt.Printf("\t\t(error) unhandled empty item type: %v\n", item.Head.Key.ItemType)
				}
			case btrfsitem.Error:
				fmt.Printf("\t\t(error) error item: %v\n", body.Err)
			default:
				fmt.Printf("\t\t(error) unhandled item type: %T\n", body)
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
	if calcSum, err := node.CalculateChecksum(); err != nil {
		fmt.Printf("checksum calced %v\n", err)
	} else {
		fmt.Printf("checksum calced %x\n", calcSum)
	}

	fmt.Printf("fs uuid %s\n", node.Head.MetadataUUID)
	fmt.Printf("chunk uuid %s\n", node.Head.ChunkTreeUUID)
}

// printExtentInlineRefs mimics part of btrfs-progs kernel-shared/print-tree.c:print_extent_item()
func printExtentInlineRefs(refs []btrfsitem.ExtentInlineRef) {
	for _, ref := range refs {
		switch subitem := ref.Body.(type) {
		case btrfsitem.Empty:
			switch ref.Type {
			case btrfsitem.TREE_BLOCK_REF_KEY:
				fmt.Printf("\t\ttree block backref root %v\n",
					btrfs.ObjID(ref.Offset))
			case btrfsitem.SHARED_BLOCK_REF_KEY:
				fmt.Printf("\t\tshared block backref parent %d\n",
					ref.Offset)
			default:
				fmt.Printf("\t\t(error) unexpected empty sub-item type: %v\n", ref.Type)
			}
		case btrfsitem.ExtentDataRef:
			fmt.Printf("\t\textent data backref root %v objectid %d offset %d count %d\n",
				subitem.Root, subitem.ObjectID, subitem.Offset, subitem.Count)
		case btrfsitem.SharedDataRef:
			fmt.Printf("\t\tshared data backref parent %d count %d\n",
				ref.Offset, subitem.Count)
		default:
			fmt.Printf("\t\t(error) unexpected sub-item type: %T\n", subitem)
		}
	}
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
