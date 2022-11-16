#!/bin/bash
set -e
b=../scratch/dump-zero
gen() (
	local tgt=$1; shift
	local log=${tgt%.*}.log
	if test -s "$tgt"; then
		return
	fi
	{ set -x; command time --verbose "$@"; } \
	     >"$tgt" \
	     2> >(tee >&2 "$log")
)

set -x
go build ./cmd/btrfs-rec
mkdir -p "$b.gen"
{ set +x; } &>/dev/null

gen $b.gen/0.scandevices.json \
    ./btrfs-rec --pv=$b.img \
    inspect scandevices
gen $b.gen/1.mappings.json \
    ./btrfs-rec --pv=$b.img \
    inspect rebuild-mappings $b.gen/0.scandevices.json

# 1.mappings.log says:
#
#     detailed report:
#     ... unmapped physical region: dev=1 beg=0x0000000000000000 end=0x0000000000100000 (size=0x0000000000100000)
#     ... unmapped physical region: dev=1 beg=0x0000000000500000 end=0x0000000001500000 (size=0x0000000001000000)
#     ... unmapped physical region: dev=1 beg=0x0000003b9e650000 end=0x0000003b9e656000 (size=0x0000000000006000)
#     ... umapped summed logical region:  beg=0x0000000000d00000 end=0x0000000001500000 (size=0x0000000000800000)
#     ... umapped block group:            beg=0x0000000000500000 end=0x0000000000d00000 (size=0x0000000000800000) flags=METADATA|single
#     ... umapped block group:            beg=0x0000000000d00000 end=0x0000000001500000 (size=0x0000000000800000) flags=DATA|single
#
# Those 2 block groups clearly both slot in to that 2nd physical
# region (which is roughly beg=5MB end=22MB).  Since that whole
# region's data was lost (roughly the 1st 100MB was lost), it doesn't
# matter which order we put the 2 block groups in within that physical
# region.  So just put them in laddr order.
#
# And then run that through `rebuild-mappings` again to fill in the
# flags and normalize it.
gen $b.gen/2.mappings.json \
    ./btrfs-rec --pv=$b.img --mappings=<(sed <$b.gen/1.mappings.json \
      -e '2a{"LAddr":5242880,"PAddr":{"Dev":1,"Addr":5242880},"Size":1},' \
      -e '2a{"LAddr":13631488,"PAddr":{"Dev":1,"Addr":13631488},"Size":1},') \
    inspect rebuild-mappings $b.gen/0.scandevices.json

# gen $b.gen/2.loops.txt \
#     ./btrfs-rec --pv=$b.img --mappings=$b.gen/2.mappings.json \
#     inspect show-loops $b.gen/0.scandevices.json
# gen $b.gen/2.nodes.json \
#     ./btrfs-rec --pv=$b.img --mappings=$b.gen/2.mappings.json \
#     inspect rebuild-nodes $b.gen/0.scandevices.json

gen $b.gen/4.ls-files.txt \
    ./btrfs-rec --pv=$b.img --mappings=$b.gen/2.mappings.json \
    inspect ls-files
gen $b.gen/4.ls-trees.txt \
    ./btrfs-rec --pv=$b.img --mappings=$b.gen/2.mappings.json \
    inspect ls-trees --scandevices=$b.gen/0.scandevices.json
