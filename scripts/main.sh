#!/bin/bash
set -e
b=../scratch/dump-zero
gen() (
	local tgt=$1; shift
	local log=${tgt%.*}.log
	if test -s "$tgt"; then
		return
	fi
	set -x
	time "$@" \
	     >"$tgt" \
	     2> >(tee >&2 "$log")
)

set -x
go build ./cmd/btrfs-rec
mkdir -p "$b.gen"
{ set +x; } &>/dev/null

gen $b.gen/0.scan-for-nodes.json \
    ./btrfs-rec --pv=$b.img \
    inspect scan-for-nodes
gen $b.gen/1.mappings.json \
    ./btrfs-rec --pv=$b.img \
    inspect rebuild-mappings $b.gen/0.scan-for-nodes.json
gen $b.gen/2.csums.gob \
    ./btrfs-rec --pv=$b.img --mappings=$b.gen/1.mappings.json \
    inspect dump-sums
gen $b.gen/3.mappings.json \
    ./btrfs-rec --pv=$b.img --mappings=$b.gen/1.mappings.json \
    inspect scan-for-extents $b.gen/0.scan-for-nodes.json $b.gen/2.csums.gob

#time ./btrfs-rec --pv=$b.img --mappings=$b.mappings.0.json inspect ls-files \
#     &> $b.ls-files.txt
