#!/bin/bash
readonly image=../scratch/dump-zero.img

######################################################################

set -e

run-btrfs-rec() {
	local tgt=$1; shift
	local log=${tgt%.*}.log
	if test -s "$tgt"; then
		return
	fi
	{
		set -x;
		command time --verbose \
			./bin/btrfs-rec \
			--pv="$image" \
			--profile.cpu="${tgt%.*}.cpu.pprof" \
			--profile.allocs="${tgt%.*}.allocs.pprof" \
			"$@"
	} >"$tgt" 2> >(tee >&2 "$log")
}

set -x
make build
gendir="${image%.img}.gen"
mkdir -p "$gendir"
export GOMEMLIMIT="$(awk '/^MemTotal:/{ print $2 "KiB" }' </proc/meminfo)"
{ set +x; } &>/dev/null

######################################################################

# 0: initial scan ##########################################

run-btrfs-rec $gendir/0.scandevices.json \
    inspect rebuild-mappings scan
run-btrfs-rec $gendir/0.nodes.json \
    inspect rebuild-mappings list-nodes $gendir/0.scandevices.json

# 1-2: rebuild chunk/dev-ext/blockgroup trees ##############

run-btrfs-rec $gendir/1.mappings.json \
    inspect rebuild-mappings process $gendir/0.scandevices.json

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
# And then run that through `rebuild-mappings process` again to fill in the
# flags and normalize it.
run-btrfs-rec $gendir/2.mappings.json \
    --mappings=<(sed <$gendir/1.mappings.json \
      -e '2a{"LAddr":5242880,"PAddr":{"Dev":1,"Addr":5242880},"Size":1},' \
      -e '2a{"LAddr":13631488,"PAddr":{"Dev":1,"Addr":13631488},"Size":1},') \
    inspect rebuild-mappings process $gendir/0.scandevices.json

# 3: rebuild other trees ###################################

run-btrfs-rec $gendir/3.trees.json \
    --mappings=$gendir/2.mappings.json \
    --node-list=$gendir/0.nodes.json \
    inspect rebuild-trees

# 4: dump data from the FS #################################

run-btrfs-rec $gendir/4.ls-files.txt \
    --mappings=$gendir/2.mappings.json \
    --node-list=$gendir/0.nodes.json \
    --rebuild \
    inspect ls-files

run-btrfs-rec $gendir/4.ls-trees.txt \
    --mappings=$gendir/2.mappings.json \
    --node-list=$gendir/0.nodes.json \
    --rebuild \
    inspect ls-trees
