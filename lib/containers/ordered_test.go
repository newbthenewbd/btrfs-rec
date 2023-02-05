// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers_test

import (
	"net/netip"

	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

var _ containers.Ordered[netip.Addr] = netip.Addr{}
