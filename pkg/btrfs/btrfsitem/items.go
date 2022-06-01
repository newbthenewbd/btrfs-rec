package btrfsitem

import (
	"fmt"
	"reflect"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

type Type = internal.ItemType

type Item interface {
	isItem()
}

type Error struct {
	Dat []byte
	Err error
}

func (Error) isItem() {}

func (o Error) MarshalBinary() ([]byte, error) {
	return o.Dat, nil
}

func (o *Error) UnmarshalBinary(dat []byte) (int, error) {
	o.Dat = dat
	return len(dat), nil
}

// Rather than returning a separate error  value, return an Error item.
func UnmarshalItem(keytyp Type, dat []byte) Item {
	gotyp, ok := keytype2gotype[keytyp]
	if !ok {
		return Error{
			Dat: dat,
			Err: fmt.Errorf("btrfsitem.UnmarshalItem(typ=%v, dat): unknown item type", keytyp),
		}
	}
	retPtr := reflect.New(gotyp)
	n, err := binstruct.Unmarshal(dat, retPtr.Interface())
	if err != nil {
		return Error{
			Dat: dat,
			Err: fmt.Errorf("btrfsitem.UnmarshalItem(typ=%v, dat): %w", keytyp, err),
		}

	}
	if n < len(dat) {
		return Error{
			Dat: dat,
			Err: fmt.Errorf("btrfsitem.UnmarshalItem(typ=%v, dat): left over data: got %d bytes but only consumed %d",
				keytyp, len(dat), n),
		}
	}
	return retPtr.Elem().Interface().(Item)
}
