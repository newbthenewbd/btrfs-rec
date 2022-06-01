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

func UnmarshalItem(keytyp Type, dat []byte) (Item, error) {
	gotyp, ok := keytype2gotype[keytyp]
	if !ok {
		return nil, fmt.Errorf("btrfsitem.UnmarshalItem: unknown item type: %v", keytyp)
	}
	retPtr := reflect.New(gotyp)
	n, err := binstruct.Unmarshal(dat, retPtr.Interface())
	if err != nil {
		return nil, fmt.Errorf("btrfsitem.UnmarshalItem: %w", err)
	}
	if n < len(dat) {
		return nil, fmt.Errorf("btrfsitem.UnmarshalItem: left over data: got %d bytes but only consumed %d",
			len(dat), n)
	}
	return retPtr.Elem().Interface().(Item), nil
}
