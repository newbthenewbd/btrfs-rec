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
gen $b.gen/2.nodes.zip \
    ./btrfs-rec --pv=$b.img --mappings=$b.gen/1.mappings.json \
    inspect visualize-nodes $b.gen/0.scandevices.json
# gen $b.gen/2.nodes.json \
#     ./btrfs-rec --pv=$b.img --mappings=$b.gen/1.mappings.json \
#     inspect rebuild-nodes $b.gen/0.scandevices.json

# gen $b.gen/3.ls-files.txt \
#     ./btrfs-rec --pv=$b.img --mappings=$b.gen/1.mappings.json \
#     inspect ls-files
# gen $b.gen/3.ls-trees.txt \
#     ./btrfs-rec --pv=$b.img --mappings=$b.gen/1.mappings.json \
#     inspect ls-trees
