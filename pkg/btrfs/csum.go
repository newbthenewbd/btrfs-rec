package btrfs

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"

	"lukeshu.com/btrfs-tools/pkg/util"
)

type CSum [0x20]byte

func (csum CSum) String() string {
	return hex.EncodeToString(csum[:])
}

func (csum CSum) Fmt(typ CSumType) string {
	return hex.EncodeToString(csum[:typ.Size()])
}

func (csum CSum) Format(f fmt.State, verb rune) {
	util.FormatByteArrayStringer(csum, csum[:], f, verb)
}

type CSumType uint16

const (
	CSUM_TYPE_CRC32 = CSumType(iota)
	CSUM_TYPE_XXHASH
	CSUM_TYPE_SHA256
	CSUM_TYPE_BLAKE2
)

func (typ CSumType) String() string {
	names := map[CSumType]string{
		CSUM_TYPE_CRC32:  "crc32c",
		CSUM_TYPE_XXHASH: "xxhash64",
		CSUM_TYPE_SHA256: "sha256",
		CSUM_TYPE_BLAKE2: "blake2",
	}
	if name, ok := names[typ]; ok {
		return name
	}
	return fmt.Sprintf("%d", typ)
}

func (typ CSumType) Size() int {
	sizes := map[CSumType]int{
		CSUM_TYPE_CRC32:  4,
		CSUM_TYPE_XXHASH: 8,
		CSUM_TYPE_SHA256: 32,
		CSUM_TYPE_BLAKE2: 32,
	}
	if size, ok := sizes[typ]; ok {
		return size
	}
	return len(CSum{})
}

func (typ CSumType) Sum(data []byte) (CSum, error) {
	switch typ {
	case CSUM_TYPE_CRC32:
		crc := crc32.Update(0, crc32.MakeTable(crc32.Castagnoli), data)

		var ret CSum
		binary.LittleEndian.PutUint32(ret[:], crc)
		return ret, nil
	case CSUM_TYPE_XXHASH:
		panic("not implemented")
	case CSUM_TYPE_SHA256:
		panic("not implemented")
	case CSUM_TYPE_BLAKE2:
		panic("not implemented")
	default:
		return CSum{}, fmt.Errorf("unknown checksum type: %v", typ)
	}
}
