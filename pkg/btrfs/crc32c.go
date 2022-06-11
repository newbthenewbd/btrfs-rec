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

func (csum CSum) Format(f fmt.State, verb rune) {
	util.FormatByteArrayStringer(csum, csum[:], f, verb)
}

func CRC32c(data []byte) CSum {
	crc := crc32.Update(0, crc32.MakeTable(crc32.Castagnoli), data)

	var ret CSum
	binary.LittleEndian.PutUint32(ret[:], crc)
	return ret
}
