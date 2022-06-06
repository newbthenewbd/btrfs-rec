package btrfs

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type CSum [0x20]byte

func (csum CSum) String() string {
	return fmt.Sprintf("%x", csum)
}

func CRC32c(data []byte) CSum {
	crc := crc32.Update(0, crc32.MakeTable(crc32.Castagnoli), data)

	var ret CSum
	binary.LittleEndian.PutUint32(ret[:], crc)
	return ret
}
