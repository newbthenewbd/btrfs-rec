#!/bin/bash
readonly image=../scratch/dump-zero.img

######################################################################

set -e
set -x

make build

gendir="${image%.img}.gen"
mountpoint="${image%.img}.mnt"
mkdir -p "$mountpoint"

sudo ./bin/btrfs-rec \
     --pv="$image" \
     --mappings="$gendir/2.mappings.json" \
     --node-list="$gendir/0.nodes.json" \
     --trees="$gendir/3.trees.json" \
     inspect mount \
     --skip-filesums \
     "$mountpoint"
