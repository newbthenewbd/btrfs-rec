#!/bin/bash
set -e
rm -f ../scratch/dump-scratch.img
cp --reflink=always ../scratch/dump-{zero,scratch}.img
chmod 600 ../scratch/dump-scratch.img
make -C ../btrfs-progs
time ../btrfs-progs/btrfs rescue chunk-recover ../scratch/dump-scratch.img
