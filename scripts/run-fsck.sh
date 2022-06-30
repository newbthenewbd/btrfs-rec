#!/bin/bash
set -e
rm -f ../scratch/dump-scratch.img
cp --reflink=always ../scratch/dump-{zero,scratch}.img
chmod 600 ../scratch/dump-scratch.img
time go run ./cmd/btrfs-fsck ../scratch/dump-scratch.img
#make -C ../btrfs-progs
#time ../btrfs-progs/btrfs rescue chunk-recover ../scratch/dump-scratch.img
#time ../btrfs-progs/btrfs check ../scratch/dump-scratch.img
