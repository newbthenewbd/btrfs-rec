package btrfsmisc

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
)

func Open(flag int, filenames ...string) (*btrfs.FS, error) {
	fs := new(btrfs.FS)
	for _, filename := range filenames {
		fh, err := os.OpenFile(filename, flag, 0)
		if err != nil {
			_ = fs.Close()
			return nil, fmt.Errorf("file %q: %w", filename, err)
		}
		if err := fs.AddDevice(&btrfs.Device{File: fh}); err != nil {
			_ = fs.Close()
			return nil, fmt.Errorf("file %q: %w", filename, err)
		}
	}
	return fs, nil
}
