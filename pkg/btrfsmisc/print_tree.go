package btrfsmisc

import (
	"fmt"
	"os"
	"strings"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/util"
)

// PrintTree mimics btrfs-progs
// kernel-shared/print-tree.c:btrfs_print_tree() and
// kernel-shared/print-tree.c:btrfs_print_leaf()
func PrintTree(fs *btrfs.FS, root btrfs.LogicalAddr) error {
	nodeRef, err := fs.ReadNode(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	}
	if nodeRef == nil {
		return nil
	}
	node := nodeRef.Data
	printHeaderInfo(node)
	if node.Head.Level > 0 { // internal
		for _, item := range node.BodyInternal {
			fmt.Printf("\t%s block %d gen %d\n",
				FmtKey(item.Key),
				item.BlockPtr,
				item.Generation)
		}
		for _, item := range node.BodyInternal {
			if err := PrintTree(fs, item.BlockPtr); err != nil {
				return err
			}
		}
	} else { // leaf
		for i, item := range node.BodyLeaf {
			fmt.Printf("\titem %d %s itemoff %d itemsize %d\n",
				i,
				FmtKey(item.Head.Key),
				item.Head.DataOffset,
				item.Head.DataSize)
			switch body := item.Body.(type) {
			case btrfsitem.FreeSpaceHeader:
				fmt.Printf("\t\tlocation %s\n", FmtKey(body.Location))
				fmt.Printf("\t\tcache generation %d entries %d bitmaps %d\n",
					body.Generation, body.NumEntries, body.NumBitmaps)
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
						FmtKey(dir.Location), dir.Type)
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
					FmtKey(body.DropProgress), body.DropLevel)
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
						FmtKey(body.Info.Key), body.Info.Level)
				}
				printExtentInlineRefs(body.Refs)
			case btrfsitem.Metadata:
				fmt.Printf("\t\trefs %d gen %d flags %v\n",
					body.Head.Refs, body.Head.Generation, body.Head.Flags)
				fmt.Printf("\t\ttree block skinny level %d\n", item.Head.Key.Offset)
				printExtentInlineRefs(body.Refs)
			//case btrfsitem.EXTENT_DATA_REF_KEY:
			//	// TODO
			//case btrfsitem.SHARED_DATA_REF_KEY:
			//	// TODO
			//case btrfsitem.EXTENT_CSUM_KEY:
			//	// TODO
			case btrfsitem.FileExtent:
				fmt.Printf("\t\tgeneration %d type %v\n",
					body.Generation, body.Type)
				switch body.Type {
				case btrfsitem.FILE_EXTENT_INLINE:
					fmt.Printf("\t\tinline extent data size %d ram_bytes %d compression %v\n",
						len(body.BodyInline), body.RAMBytes, body.Compression)
				case btrfsitem.FILE_EXTENT_PREALLOC:
					fmt.Printf("\t\tprealloc data disk byte %d nr %d\n",
						body.BodyPrealloc.DiskByteNr,
						body.BodyPrealloc.DiskNumBytes)
					fmt.Printf("\t\tprealloc data offset %d nr %d\n",
						body.BodyPrealloc.Offset,
						body.BodyPrealloc.NumBytes)
				case btrfsitem.FILE_EXTENT_REG:
					fmt.Printf("\t\textent data disk byte %d nr %d\n",
						body.BodyReg.DiskByteNr,
						body.BodyReg.DiskNumBytes)
					fmt.Printf("\t\textenti data offset %d nr %d ram %d\n",
						body.BodyReg.Offset,
						body.BodyReg.NumBytes,
						body.RAMBytes)
					fmt.Printf("\t\textent compression %v\n",
						body.Compression)
				default:
					fmt.Printf("\t\t(error) unknown file extent type %v", body.Type)
				}
			case btrfsitem.BlockGroup:
				fmt.Printf("\t\tblock group used %d chunk_objectid %d flags %v\n",
					body.Used, body.ChunkObjectID, body.Flags)
			case btrfsitem.FreeSpaceInfo:
				fmt.Printf("\t\tfree space info extent count %d flags %d\n",
					body.ExtentCount, body.Flags)
			case btrfsitem.FreeSpaceBitmap:
				fmt.Printf("\t\tfree space bitmap\n")
			case btrfsitem.Chunk:
				fmt.Printf("\t\tlength %d owner %d stripe_len %d type %v\n",
					body.Head.Size, body.Head.Owner, body.Head.StripeLen, body.Head.Type)
				fmt.Printf("\t\tio_align %d io_width %d sector_size %d\n",
					body.Head.IOOptimalAlign, body.Head.IOOptimalWidth, body.Head.IOMinSize)
				fmt.Printf("\t\tnum_stripes %d sub_stripes %d\n",
					body.Head.NumStripes, body.Head.SubStripes)
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
			case btrfsitem.DevExtent:
				fmt.Printf(""+
					"\t\tdev extent chunk_tree %d\n"+
					"\t\tchunk_objectid %d chunk_offset %d length %d\n"+
					"\t\tchunk_tree_uuid %s\n",
					body.ChunkTree, body.ChunkObjectID, body.ChunkOffset, body.Length,
					body.ChunkTreeUUID)
			//case btrfsitem.QGROUP_STATUS_KEY:
			//	// TODO
			//case btrfsitem.QGROUP_INFO_KEY:
			//	// TODO
			//case btrfsitem.QGROUP_LIMIT_KEY:
			//	// TODO
			case btrfsitem.UUIDMap:
				for _, subvolID := range body {
					fmt.Printf("\t\tsubvol_id %d\n",
						subvolID)
				}
			//case btrfsitem.STRING_ITEM_KEY:
			//	// TODO
			case btrfsitem.DevStats:
				fmt.Printf("\t\tpersistent item objectid %s offset %d\n",
					item.Head.Key.ObjectID.Format(item.Head.Key.ItemType), item.Head.Key.Offset)
				switch item.Head.Key.ObjectID {
				case btrfs.DEV_STATS_OBJECTID:
					fmt.Printf("\t\tdevice stats\n")
					fmt.Printf("\t\twrite_errs %d read_errs %d flush_errs %d corruption_errs %d generation %d\n",
						body.Values[btrfsitem.DEV_STAT_WRITE_ERRS],
						body.Values[btrfsitem.DEV_STAT_READ_ERRS],
						body.Values[btrfsitem.DEV_STAT_FLUSH_ERRS],
						body.Values[btrfsitem.DEV_STAT_CORRUPTION_ERRS],
						body.Values[btrfsitem.DEV_STAT_GENERATION_ERRS])
				default:
					fmt.Printf("\t\tunknown persistent item objectid %d\n", item.Head.Key.ObjectID)
				}
			//case btrfsitem.TEMPORARY_ITEM_KEY:
			//	// TODO
			case btrfsitem.Empty:
				switch item.Head.Key.ItemType {
				case btrfsitem.ORPHAN_ITEM_KEY: // 48
					fmt.Printf("\t\torphan item\n")
				case btrfsitem.TREE_BLOCK_REF_KEY: // 176
					fmt.Printf("\t\ttree block backref\n")
				case btrfsitem.SHARED_BLOCK_REF_KEY: // 182
					fmt.Printf("\t\tshared block backref\n")
				case btrfsitem.FREE_SPACE_EXTENT_KEY: // 199
					fmt.Printf("\t\tfree space extent\n")
				case btrfsitem.QGROUP_RELATION_KEY: // 246
					// do nothing
				//case btrfsitem.EXTENT_REF_V0_KEY:
				//	fmt.Printf("\t\textent ref v0 (deprecated)\n")
				//case btrfsitem.CSUM_ITEM_KEY:
				//	fmt.Printf("\t\tcsum item\n")
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

	fmt.Printf("checksum stored %v\n", node.Head.Checksum)
	if calcSum, err := node.CalculateChecksum(); err != nil {
		fmt.Printf("checksum calced %v\n", err)
	} else {
		fmt.Printf("checksum calced %v\n", calcSum)
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
func FmtKey(key btrfs.Key) string {
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
