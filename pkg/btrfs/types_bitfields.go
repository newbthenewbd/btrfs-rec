package btrfs

import (
	"encoding/binary"
	"fmt"
	"strings"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

func bitfieldString[T ~uint8 | ~uint16 | ~uint32 | ~uint64](bitfield T, bitnames []string) string {
	if bitfield == 0 {
		return "0"
	}
	var out strings.Builder
	fmt.Fprintf(&out, "0x%0X", uint64(bitfield))
	if bitfield == 0 {
		out.WriteString("(none)")
	} else {
		rest := bitfield
		sep := '('
		for i := 0; rest != 0; i++ {
			if rest&(1<<i) != 0 {
				out.WriteRune(sep)
				if i < len(bitnames) {
					out.WriteString(bitnames[i])
				} else {
					fmt.Fprintf(&out, "(1<<%d)", i)
				}
				sep = '|'
			}
			rest &^= 1 << i
		}
		out.WriteRune(')')
	}
	return out.String()
}

type IncompatFlags uint64

const (
	FeatureIncompatMixedBackref = IncompatFlags(1 << iota)
	FeatureIncompatDefaultSubvol
	FeatureIncompatMixedGroups
	FeatureIncompatCompressLZO
	FeatureIncompatCompressZSTD
	FeatureIncompatBigMetadata // buggy
	FeatureIncompatExtendedIRef
	FeatureIncompatRAID56
	FeatureIncompatSkinnyMetadata
	FeatureIncompatNoHoles
	FeatureIncompatMetadataUUID
	FeatureIncompatRAID1C34
	FeatureIncompatZoned
	FeatureIncompatExtentTreeV2
)

var incompatFlagNames = []string{
	"FeatureIncompatMixedBackref",
	"FeatureIncompatDefaultSubvol",
	"FeatureIncompatMixedGroups",
	"FeatureIncompatCompressLZO",
	"FeatureIncompatCompressZSTD",
	"FeatureIncompatBigMetadata ",
	"FeatureIncompatExtendedIRef",
	"FeatureIncompatRAID56",
	"FeatureIncompatSkinnyMetadata",
	"FeatureIncompatNoHoles",
	"FeatureIncompatMetadataUUID",
	"FeatureIncompatRAID1C34",
	"FeatureIncompatZoned",
	"FeatureIncompatExtentTreeV2",
}

func (f IncompatFlags) Has(req IncompatFlags) bool { return f&req == req }
func (f IncompatFlags) String() string             { return bitfieldString(f, incompatFlagNames) }

type NodeFlags uint64

func (NodeFlags) BinarySize() int64 {
	return 7
}
func (f NodeFlags) MarshalBinary() []byte {
	var bs [8]byte
	binary.LittleEndian.PutUint64(bs[:], uint64(f))
	return bs[:7]
}
func (f *NodeFlags) UnmarshalBinary(dat []byte) {
	var bs [8]byte
	copy(bs[:7], dat[:7])
	*f = NodeFlags(binary.LittleEndian.Uint64(bs[:]))
}

var (
	_ binstruct.Marshaler   = NodeFlags(0)
	_ binstruct.Unmarshaler = (*NodeFlags)(nil)
)

const (
	NodeWritten = NodeFlags(1 << iota)
	NodeReloc
)

var nodeFlagNames = []string{
	"WRITTEN",
	"RELOC",
}

func (f NodeFlags) Has(req NodeFlags) bool { return f&req == req }
func (f NodeFlags) String() string         { return bitfieldString(f, nodeFlagNames) }
