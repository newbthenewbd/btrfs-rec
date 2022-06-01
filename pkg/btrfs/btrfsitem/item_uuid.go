package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

// The Key for this item is a UUID, and the item is a list of
// subvolume IDs (ObjectIDs) that that UUID maps to.
type UUIDMap struct { // UUID_SUBVOL=251 UUID_RECEIVED_SUBVOL=252
	SubvolIDs []internal.ObjID
}

func (o *UUIDMap) UnmarshalBinary(dat []byte) (int, error) {
	o.SubvolIDs = nil
	var n int
	for len(dat) > n {
		var subvolID internal.ObjID
		_n, err := binstruct.Unmarshal(dat[n:], &subvolID)
		n += _n
		if err != nil {
			return n, err
		}
		o.SubvolIDs = append(o.SubvolIDs, subvolID)
	}
	return n, nil
}

func (o UUIDMap) MarshalBinary() ([]byte, error) {
	var ret []byte
	for _, subvolID := range o.SubvolIDs {
		bs, err := binstruct.Marshal(subvolID)
		ret = append(ret, bs...)
		if err != nil {
			return ret, err
		}
	}
	return ret, nil
}
