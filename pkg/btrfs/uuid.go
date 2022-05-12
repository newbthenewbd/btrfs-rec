package btrfs

import (
	"bytes"
	"encoding/hex"
	"strings"
)

type UUID [16]byte

func (uuid UUID) String() string {
	str := hex.EncodeToString(uuid[:])
	return strings.Join([]string{
		str[:8],
		str[8:12],
		str[12:16],
		str[16:20],
		str[20:32],
	}, "-")
}

func (a UUID) Equal(b UUID) bool {
	return bytes.Equal(a[:], b[:])
}
