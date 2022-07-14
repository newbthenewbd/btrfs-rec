// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package diskio

type statefulFile[A ~int64] struct {
	inner File[A]
	pos   A
}

var _ File[assertAddr] = (*statefulFile[assertAddr])(nil)

func NewStatefulFile[A ~int64](file File[A]) *statefulFile[A] {
	return &statefulFile[A]{
		inner: file,
	}
}

func (sf *statefulFile[A]) Name() string                           { return sf.inner.Name() }
func (sf *statefulFile[A]) Size() A                                { return sf.inner.Size() }
func (sf *statefulFile[A]) Close() error                           { return sf.inner.Close() }
func (sf *statefulFile[A]) ReadAt(dat []byte, off A) (int, error)  { return sf.inner.ReadAt(dat, off) }
func (sf *statefulFile[A]) WriteAt(dat []byte, off A) (int, error) { return sf.inner.WriteAt(dat, off) }

func (sf *statefulFile[A]) Read(dat []byte) (n int, err error) {
	n, err = sf.ReadAt(dat, sf.pos)
	sf.pos += A(n)
	return n, err
}

func (sf *statefulFile[A]) ReadByte() (byte, error) {
	var dat [1]byte
	_, err := sf.Read(dat[:])
	return dat[0], err
}
