// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package dumptrees

import (
	"context"
	"io"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func DumpTrees(ctx context.Context, out io.Writer, fs *btrfs.FS) {
	superblock, err := fs.Superblock()
	if err != nil {
		dlog.Error(ctx, err)
		return
	}

	if superblock.RootTree != 0 {
		textui.Fprintf(out, "root tree\n")
		printTree(ctx, out, fs, btrfsprim.ROOT_TREE_OBJECTID)
	}
	if superblock.ChunkTree != 0 {
		textui.Fprintf(out, "chunk tree\n")
		printTree(ctx, out, fs, btrfsprim.CHUNK_TREE_OBJECTID)
	}
	if superblock.LogTree != 0 {
		textui.Fprintf(out, "log root tree\n")
		printTree(ctx, out, fs, btrfsprim.TREE_LOG_OBJECTID)
	}
	if superblock.BlockGroupRoot != 0 {
		textui.Fprintf(out, "block group tree\n")
		printTree(ctx, out, fs, btrfsprim.BLOCK_GROUP_TREE_OBJECTID)
	}
	fs.TreeWalk(
		ctx,
		btrfsprim.ROOT_TREE_OBJECTID,
		func(err *btrfstree.TreeError) {
			dlog.Error(ctx, err)
		},
		btrfstree.TreeWalkHandler{
			Item: func(_ btrfstree.TreePath, item btrfstree.Item) error {
				if item.Key.ItemType != btrfsitem.ROOT_ITEM_KEY {
					return nil
				}
				treeName, ok := map[btrfsprim.ObjID]string{
					btrfsprim.ROOT_TREE_OBJECTID:        "root",
					btrfsprim.EXTENT_TREE_OBJECTID:      "extent",
					btrfsprim.CHUNK_TREE_OBJECTID:       "chunk",
					btrfsprim.DEV_TREE_OBJECTID:         "device",
					btrfsprim.FS_TREE_OBJECTID:          "fs",
					btrfsprim.ROOT_TREE_DIR_OBJECTID:    "directory",
					btrfsprim.CSUM_TREE_OBJECTID:        "checksum",
					btrfsprim.ORPHAN_OBJECTID:           "orphan",
					btrfsprim.TREE_LOG_OBJECTID:         "log",
					btrfsprim.TREE_LOG_FIXUP_OBJECTID:   "log fixup",
					btrfsprim.TREE_RELOC_OBJECTID:       "reloc",
					btrfsprim.DATA_RELOC_TREE_OBJECTID:  "data reloc",
					btrfsprim.EXTENT_CSUM_OBJECTID:      "extent checksum",
					btrfsprim.QUOTA_TREE_OBJECTID:       "quota",
					btrfsprim.UUID_TREE_OBJECTID:        "uuid",
					btrfsprim.FREE_SPACE_TREE_OBJECTID:  "free space",
					btrfsprim.MULTIPLE_OBJECTIDS:        "multiple",
					btrfsprim.BLOCK_GROUP_TREE_OBJECTID: "block group",
				}[item.Key.ObjectID]
				if !ok {
					treeName = "file"
				}
				textui.Fprintf(out, "%v tree key %v \n", treeName, item.Key.Format(btrfsprim.ROOT_TREE_OBJECTID))
				printTree(ctx, out, fs, item.Key.ObjectID)
				return nil
			},
		},
	)
	textui.Fprintf(out, "total bytes %v\n", superblock.TotalBytes)
	textui.Fprintf(out, "bytes used %v\n", superblock.BytesUsed)
	textui.Fprintf(out, "uuid %v\n", superblock.FSUUID)
}

var nodeHeaderSize = binstruct.StaticSize(btrfstree.NodeHeader{})

// printTree mimics btrfs-progs
// kernel-shared/print-tree.c:btrfs_print_tree() and
// kernel-shared/print-tree.c:btrfs_print_leaf()
func printTree(ctx context.Context, out io.Writer, fs *btrfs.FS, treeID btrfsprim.ObjID) {
	var itemOffset uint32
	handlers := btrfstree.TreeWalkHandler{
		Node: func(path btrfstree.TreePath, nodeRef *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
			printHeaderInfo(out, nodeRef.Data)
			itemOffset = nodeRef.Data.Size - uint32(nodeHeaderSize)
			return nil
		},
		PreKeyPointer: func(path btrfstree.TreePath, item btrfstree.KeyPointer) error {
			treeID := path[0].FromTree
			textui.Fprintf(out, "\tkey %v block %v gen %v\n",
				item.Key.Format(treeID),
				item.BlockPtr,
				item.Generation)
			return nil
		},
		Item: func(path btrfstree.TreePath, item btrfstree.Item) error {
			treeID := path[0].FromTree
			i := path.Node(-1).FromItemIdx
			bs, _ := binstruct.Marshal(item.Body)
			itemSize := uint32(len(bs))
			itemOffset -= itemSize
			textui.Fprintf(out, "\titem %v key %v itemoff %v itemsize %v\n",
				i,
				item.Key.Format(treeID),
				itemOffset,
				itemSize)
			switch body := item.Body.(type) {
			case *btrfsitem.FreeSpaceHeader:
				textui.Fprintf(out, "\t\tlocation key %v\n", body.Location.Format(treeID))
				textui.Fprintf(out, "\t\tcache generation %v entries %v bitmaps %v\n",
					body.Generation, body.NumEntries, body.NumBitmaps)
			case *btrfsitem.Inode:
				textui.Fprintf(out, ""+
					"\t\tgeneration %v transid %v size %v nbytes %v\n"+
					"\t\tblock group %v mode %o links %v uid %v gid %v rdev %v\n"+
					"\t\tsequence %v flags %v\n",
					body.Generation, body.TransID, body.Size, body.NumBytes,
					body.BlockGroup, body.Mode, body.NLink, body.UID, body.GID, body.RDev,
					body.Sequence, body.Flags)
				textui.Fprintf(out, "\t\tatime %v\n", fmtTime(body.ATime))
				textui.Fprintf(out, "\t\tctime %v\n", fmtTime(body.CTime))
				textui.Fprintf(out, "\t\tmtime %v\n", fmtTime(body.MTime))
				textui.Fprintf(out, "\t\totime %v\n", fmtTime(body.OTime))
			case *btrfsitem.InodeRefs:
				for _, ref := range body.Refs {
					textui.Fprintf(out, "\t\tindex %v namelen %v name: %s\n",
						ref.Index, ref.NameLen, ref.Name)
				}
			// case btrfsitem.INODE_EXTREF_KEY:
			// 	// TODO
			case *btrfsitem.DirEntry:
				textui.Fprintf(out, "\t\tlocation key %v type %v\n",
					body.Location.Format(treeID), body.Type)
				textui.Fprintf(out, "\t\ttransid %v data_len %v name_len %v\n",
					body.TransID, body.DataLen, body.NameLen)
				textui.Fprintf(out, "\t\tname: %s\n", body.Name)
				if len(body.Data) > 0 {
					textui.Fprintf(out, "\t\tdata %s\n", body.Data)
				}
			// case btrfsitem.DIR_LOG_INDEX_KEY, btrfsitem.DIR_LOG_ITEM_KEY:
			// 	// TODO
			case *btrfsitem.Root:
				textui.Fprintf(out, "\t\tgeneration %v root_dirid %v bytenr %d byte_limit %v bytes_used %v\n",
					body.Generation, body.RootDirID, body.ByteNr, body.ByteLimit, body.BytesUsed)
				textui.Fprintf(out, "\t\tlast_snapshot %v flags %v refs %v\n",
					body.LastSnapshot, body.Flags, body.Refs)
				textui.Fprintf(out, "\t\tdrop_progress key %v drop_level %v\n",
					body.DropProgress.Format(treeID), body.DropLevel)
				textui.Fprintf(out, "\t\tlevel %v generation_v2 %v\n",
					body.Level, body.GenerationV2)
				if body.Generation == body.GenerationV2 {
					textui.Fprintf(out, "\t\tuuid %v\n", body.UUID)
					textui.Fprintf(out, "\t\tparent_uuid %v\n", body.ParentUUID)
					textui.Fprintf(out, "\t\treceived_uuid %v\n", body.ReceivedUUID)
					textui.Fprintf(out, "\t\tctransid %v otransid %v stransid %v rtransid %v\n",
						body.CTransID, body.OTransID, body.STransID, body.RTransID)
					textui.Fprintf(out, "\t\tctime %v\n", fmtTime(body.CTime))
					textui.Fprintf(out, "\t\totime %v\n", fmtTime(body.OTime))
					textui.Fprintf(out, "\t\tstime %v\n", fmtTime(body.STime))
					textui.Fprintf(out, "\t\trtime %v\n", fmtTime(body.RTime))
				}
			case *btrfsitem.RootRef:
				var tag string
				switch item.Key.ItemType {
				case btrfsitem.ROOT_REF_KEY:
					tag = "ref"
				case btrfsitem.ROOT_BACKREF_KEY:
					tag = "backref"
				default:
					tag = textui.Sprintf("(error: unhandled RootRef item type: %v)", item.Key.ItemType)
				}
				textui.Fprintf(out, "\t\troot %v key dirid %v sequence %v name %s\n",
					tag, body.DirID, body.Sequence, body.Name)
			case *btrfsitem.Extent:
				textui.Fprintf(out, "\t\trefs %v gen %v flags %v\n",
					body.Head.Refs, body.Head.Generation, body.Head.Flags)
				if body.Head.Flags.Has(btrfsitem.EXTENT_FLAG_TREE_BLOCK) {
					textui.Fprintf(out, "\t\ttree block key %v level %v\n",
						body.Info.Key.Format(treeID), body.Info.Level)
				}
				printExtentInlineRefs(out, body.Refs)
			case *btrfsitem.Metadata:
				textui.Fprintf(out, "\t\trefs %v gen %v flags %v\n",
					body.Head.Refs, body.Head.Generation, body.Head.Flags)
				textui.Fprintf(out, "\t\ttree block skinny level %v\n", item.Key.Offset)
				printExtentInlineRefs(out, body.Refs)
			// case btrfsitem.EXTENT_DATA_REF_KEY:
			// 	// TODO
			// case btrfsitem.SHARED_DATA_REF_KEY:
			// 	// TODO
			case *btrfsitem.ExtentCSum:
				start := btrfsvol.LogicalAddr(item.Key.Offset)
				textui.Fprintf(out, "\t\trange start %d end %d length %d",
					start, start.Add(body.Size()), body.Size())
				sumsPerLine := slices.Max(1, len(btrfssum.CSum{})/body.ChecksumSize/2)

				i := 0
				_ = body.Walk(ctx, func(pos btrfsvol.LogicalAddr, sum btrfssum.ShortSum) error {
					if i%sumsPerLine == 0 {
						textui.Fprintf(out, "\n\t\t")
					} else {
						textui.Fprintf(out, " ")
					}
					textui.Fprintf(out, "[%d] 0x%x", pos, sum)
					i++
					return nil
				})
				textui.Fprintf(out, "\n")
			case *btrfsitem.FileExtent:
				textui.Fprintf(out, "\t\tgeneration %v type %v\n",
					body.Generation, body.Type)
				switch body.Type {
				case btrfsitem.FILE_EXTENT_INLINE:
					textui.Fprintf(out, "\t\tinline extent data size %v ram_bytes %v compression %v\n",
						len(body.BodyInline), body.RAMBytes, body.Compression)
				case btrfsitem.FILE_EXTENT_PREALLOC:
					textui.Fprintf(out, "\t\tprealloc data disk byte %v nr %v\n",
						body.BodyExtent.DiskByteNr,
						body.BodyExtent.DiskNumBytes)
					textui.Fprintf(out, "\t\tprealloc data offset %v nr %v\n",
						body.BodyExtent.Offset,
						body.BodyExtent.NumBytes)
				case btrfsitem.FILE_EXTENT_REG:
					textui.Fprintf(out, "\t\textent data disk byte %d nr %d\n",
						body.BodyExtent.DiskByteNr,
						body.BodyExtent.DiskNumBytes)
					textui.Fprintf(out, "\t\textent data offset %d nr %d ram %v\n",
						body.BodyExtent.Offset,
						body.BodyExtent.NumBytes,
						body.RAMBytes)
					textui.Fprintf(out, "\t\textent compression %v\n",
						body.Compression)
				default:
					textui.Fprintf(out, "\t\t(error) unknown file extent type %v", body.Type)
				}
			case *btrfsitem.BlockGroup:
				textui.Fprintf(out, "\t\tblock group used %v chunk_objectid %v flags %v\n",
					body.Used, body.ChunkObjectID, body.Flags)
			case *btrfsitem.FreeSpaceInfo:
				textui.Fprintf(out, "\t\tfree space info extent count %v flags %d\n",
					body.ExtentCount, body.Flags)
			case *btrfsitem.FreeSpaceBitmap:
				textui.Fprintf(out, "\t\tfree space bitmap\n")
			case *btrfsitem.Chunk:
				textui.Fprintf(out, "\t\tlength %d owner %d stripe_len %v type %v\n",
					body.Head.Size, body.Head.Owner, body.Head.StripeLen, body.Head.Type)
				textui.Fprintf(out, "\t\tio_align %v io_width %v sector_size %v\n",
					body.Head.IOOptimalAlign, body.Head.IOOptimalWidth, body.Head.IOMinSize)
				textui.Fprintf(out, "\t\tnum_stripes %v sub_stripes %v\n",
					body.Head.NumStripes, body.Head.SubStripes)
				for i, stripe := range body.Stripes {
					textui.Fprintf(out, "\t\t\tstripe %v devid %d offset %d\n",
						i, stripe.DeviceID, stripe.Offset)
					textui.Fprintf(out, "\t\t\tdev_uuid %v\n",
						stripe.DeviceUUID)
				}
			case *btrfsitem.Dev:
				textui.Fprintf(out, ""+
					"\t\tdevid %d total_bytes %v bytes_used %v\n"+
					"\t\tio_align %v io_width %v sector_size %v type %v\n"+
					"\t\tgeneration %v start_offset %v dev_group %v\n"+
					"\t\tseek_speed %v bandwidth %v\n"+
					"\t\tuuid %v\n"+
					"\t\tfsid %v\n",
					body.DevID, body.NumBytes, body.NumBytesUsed,
					body.IOOptimalAlign, body.IOOptimalWidth, body.IOMinSize, body.Type,
					body.Generation, body.StartOffset, body.DevGroup,
					body.SeekSpeed, body.Bandwidth,
					body.DevUUID,
					body.FSUUID)
			case *btrfsitem.DevExtent:
				textui.Fprintf(out, ""+
					"\t\tdev extent chunk_tree %d\n"+
					"\t\tchunk_objectid %v chunk_offset %d length %d\n"+
					"\t\tchunk_tree_uuid %v\n",
					body.ChunkTree, body.ChunkObjectID, body.ChunkOffset, body.Length,
					body.ChunkTreeUUID)
			case *btrfsitem.QGroupStatus:
				textui.Fprintf(out, ""+
					"\t\tversion %v generation %v flags %v scan %d\n",
					body.Version, body.Generation, body.Flags, body.RescanProgress)
			case *btrfsitem.QGroupInfo:
				textui.Fprintf(out, ""+
					"\t\tgeneration %v\n"+
					"\t\treferenced %d referenced_compressed %d\n"+
					"\t\texclusive %d exclusive_compressed %d\n",
					body.Generation,
					body.ReferencedBytes, body.ReferencedBytesCompressed,
					body.ExclusiveBytes, body.ExclusiveBytesCompressed)
			case *btrfsitem.QGroupLimit:
				textui.Fprintf(out, ""+
					"\t\tflags %x\n"+
					"\t\tmax_referenced %d max_exclusive %d\n"+
					"\t\trsv_referenced %d rsv_exclusive %d\n",
					uint64(body.Flags),
					body.MaxReferenced, body.MaxExclusive,
					body.RsvReferenced, body.RsvExclusive)
			case *btrfsitem.UUIDMap:
				textui.Fprintf(out, "\t\tsubvol_id %d\n", body.ObjID)
			// case btrfsitem.STRING_ITEM_KEY:
			// 	// TODO
			case *btrfsitem.DevStats:
				textui.Fprintf(out, "\t\tpersistent item objectid %v offset %v\n",
					item.Key.ObjectID.Format(treeID), item.Key.Offset)
				switch item.Key.ObjectID {
				case btrfsprim.DEV_STATS_OBJECTID:
					textui.Fprintf(out, "\t\tdevice stats\n")
					textui.Fprintf(out, "\t\twrite_errs %v read_errs %v flush_errs %v corruption_errs %v generation %v\n",
						body.Values[btrfsitem.DEV_STAT_WRITE_ERRS],
						body.Values[btrfsitem.DEV_STAT_READ_ERRS],
						body.Values[btrfsitem.DEV_STAT_FLUSH_ERRS],
						body.Values[btrfsitem.DEV_STAT_CORRUPTION_ERRS],
						body.Values[btrfsitem.DEV_STAT_GENERATION_ERRS])
				default:
					textui.Fprintf(out, "\t\tunknown persistent item objectid %v\n", item.Key.ObjectID)
				}
			// case btrfsitem.TEMPORARY_ITEM_KEY:
			// 	// TODO
			case *btrfsitem.Empty:
				switch item.Key.ItemType {
				case btrfsitem.ORPHAN_ITEM_KEY: // 48
					textui.Fprintf(out, "\t\torphan item\n")
				case btrfsitem.TREE_BLOCK_REF_KEY: // 176
					textui.Fprintf(out, "\t\ttree block backref\n")
				case btrfsitem.SHARED_BLOCK_REF_KEY: // 182
					textui.Fprintf(out, "\t\tshared block backref\n")
				case btrfsitem.FREE_SPACE_EXTENT_KEY: // 199
					textui.Fprintf(out, "\t\tfree space extent\n")
				case btrfsitem.QGROUP_RELATION_KEY: // 246
					// do nothing
				// case btrfsitem.EXTENT_REF_V0_KEY:
				// 	textui.Fprintf(out, "\t\textent ref v0 (deprecated)\n")
				// case btrfsitem.CSUM_ITEM_KEY:
				// 	textui.Fprintf(out, "\t\tcsum item\n")
				default:
					textui.Fprintf(out, "\t\t(error) unhandled empty item type: %v\n", item.Key.ItemType)
				}
			case *btrfsitem.Error:
				textui.Fprintf(out, "\t\t(error) error item: %v\n", body.Err)
			default:
				textui.Fprintf(out, "\t\t(error) unhandled item type: %T\n", body)
			}
			return nil
		},
	}
	handlers.BadItem = handlers.Item
	fs.TreeWalk(
		ctx,
		treeID,
		func(err *btrfstree.TreeError) {
			dlog.Error(ctx, err)
		},
		handlers,
	)
}

// printHeaderInfo mimics btrfs-progs kernel-shared/print-tree.c:print_header_info()
func printHeaderInfo(out io.Writer, node btrfstree.Node) {
	var typename string
	if node.Head.Level > 0 { // internal node
		typename = "node"
		textui.Fprintf(out, "node %v level %v items %v free space %v",
			node.Head.Addr,
			node.Head.Level,
			node.Head.NumItems,
			node.MaxItems()-node.Head.NumItems)
	} else { // leaf node
		typename = "leaf"
		textui.Fprintf(out, "leaf %d items %v free space %v",
			node.Head.Addr,
			node.Head.NumItems,
			node.LeafFreeSpace())
	}
	textui.Fprintf(out, " generation %v owner %v\n",
		node.Head.Generation,
		node.Head.Owner)

	textui.Fprintf(out, "%v %d flags %v backref revision %v\n",
		typename,
		node.Head.Addr,
		node.Head.Flags,
		node.Head.BackrefRev)

	textui.Fprintf(out, "checksum stored %v\n", node.Head.Checksum.Fmt(node.ChecksumType))
	if calcSum, err := node.CalculateChecksum(); err != nil {
		textui.Fprintf(out, "checksum calced %v\n", err)
	} else {
		textui.Fprintf(out, "checksum calced %v\n", calcSum.Fmt(node.ChecksumType))
	}

	textui.Fprintf(out, "fs uuid %v\n", node.Head.MetadataUUID)
	textui.Fprintf(out, "chunk uuid %v\n", node.Head.ChunkTreeUUID)
}

// printExtentInlineRefs mimics part of btrfs-progs kernel-shared/print-tree.c:print_extent_item()
func printExtentInlineRefs(out io.Writer, refs []btrfsitem.ExtentInlineRef) {
	for _, ref := range refs {
		switch subitem := ref.Body.(type) {
		case nil:
			switch ref.Type {
			case btrfsitem.TREE_BLOCK_REF_KEY:
				textui.Fprintf(out, "\t\ttree block backref root %v\n",
					btrfsprim.ObjID(ref.Offset))
			case btrfsitem.SHARED_BLOCK_REF_KEY:
				textui.Fprintf(out, "\t\tshared block backref parent %v\n",
					ref.Offset)
			default:
				textui.Fprintf(out, "\t\t(error) unexpected empty sub-item type: %v\n", ref.Type)
			}
		case *btrfsitem.ExtentDataRef:
			textui.Fprintf(out, "\t\textent data backref root %v objectid %v offset %v count %v\n",
				subitem.Root, subitem.ObjectID, subitem.Offset, subitem.Count)
		case *btrfsitem.SharedDataRef:
			textui.Fprintf(out, "\t\tshared data backref parent %v count %v\n",
				ref.Offset, subitem.Count)
		default:
			textui.Fprintf(out, "\t\t(error) unexpected sub-item type: %T\n", subitem)
		}
	}
}

func fmtTime(t btrfsprim.Time) string {
	return textui.Sprintf("%v.%v (%v)",
		t.Sec, t.NSec, t.ToStd().Format("2006-01-02 15:04:05"))
}
