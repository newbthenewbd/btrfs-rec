package btrfs

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

type CSum [0x20]byte

func (a CSum) Equal(b CSum) bool {
	return bytes.Equal(a[:], b[:])
}

func CRC32c(data []byte) CSum {
	crc := crc32.Update(0, crc32.MakeTable(crc32.Castagnoli), data)

	var ret CSum
	binary.LittleEndian.PutUint32(ret[:], crc)
	return ret
}
