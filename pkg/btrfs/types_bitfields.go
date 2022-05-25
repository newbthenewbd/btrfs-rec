package btrfs

import (
	"fmt"
	"strings"
)

func bitfieldString[T ~uint8 | ~uint16 | ~uint32 | ~uint64](bitfield T, bitnames []string) string {
	if bitfield == 0 {
		return "0"
	}
	var out strings.Builder
	fmt.Fprintf(&out, "(0x%0X)", uint64(bitfield))
	rest := bitfield
	sep := ' '
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
