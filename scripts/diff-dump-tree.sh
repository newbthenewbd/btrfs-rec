#!/bin/bash
readonly image=../scratch/new.img

######################################################################

set -e
(
	cd ../btrfs-progs
	if ! test -f configure; then
		./autogen.sh
	fi
	if ! test -f config.status; then
		./configure \
			--disable-documentation \
			--enable-experimental
	fi
	make
)

make build

######################################################################

diff -u \
     <(../btrfs-progs/btrfs inspect dump-tree --noscan --csum-items "$image" |
	       sed -e 's/ UNKNOWN.0 / UNTYPED /g' \
		   -e 's/\b18446744073709551615\b/-1/g' \
		   -e 's/INODE_REF 6)/INODE_REF ROOT_TREE_DIR)/g' \
		   -e 's/ROOT_BACKREF 5)/ROOT_BACKREF FS_TREE)/g' \
		   ) \
     <(./bin/btrfs-rec inspect dump-trees --pv="$image" |
	       sed -E \
		   -e 's/([0-9]),/\1/g' \
		   )
