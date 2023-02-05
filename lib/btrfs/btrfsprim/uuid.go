// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsprim

import (
	"encoding"
	"encoding/hex"
	"fmt"
	"strings"

	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
)

type UUID [16]byte

var (
	_ fmt.Stringer             = UUID{}
	_ fmt.Formatter            = UUID{}
	_ encoding.TextMarshaler   = UUID{}
	_ encoding.TextUnmarshaler = (*UUID)(nil)
)

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

func (uuid UUID) MarshalText() ([]byte, error) {
	return []byte(uuid.String()), nil
}

func (uuid *UUID) UnmarshalText(text []byte) error {
	var err error
	*uuid, err = ParseUUID(string(text))
	return err
}

func (uuid UUID) Format(f fmt.State, verb rune) {
	fmtutil.FormatByteArrayStringer(uuid, uuid[:], f, verb)
}

func (a UUID) Compare(b UUID) int {
	for i := range a {
		if d := int(a[i]) - int(b[i]); d != 0 {
			return d
		}
	}
	return 0
}

//nolint:gomnd // This is all magic numbers.
func ParseUUID(str string) (UUID, error) {
	var ret UUID
	j := 0
	for i := 0; i < len(str); i++ {
		if j >= len(ret)*2 {
			return UUID{}, fmt.Errorf("too long to be a UUID: %q|%q", str[:i], str[i:])
		}
		c := str[i]
		var v byte
		switch {
		case '0' <= c && c <= '9':
			v = c - '0'
		case 'a' <= c && c <= 'f':
			v = c - 'a' + 10
		case 'A' <= c && c <= 'F':
			v = c - 'A' + 10
		case c == '-':
			continue
		default:
			return UUID{}, fmt.Errorf("illegal byte in UUID: %q|%q|%q", str[:i], str[i:i+1], str[i+1:])
		}
		if j%2 == 0 {
			ret[j/2] = v << 4
		} else {
			ret[j/2] = (ret[j/2] & 0xf0) | (v & 0x0f)
		}
		j++
	}
	return ret, nil
}

func MustParseUUID(str string) UUID {
	ret, err := ParseUUID(str)
	if err != nil {
		panic(err)
	}
	return ret
}
