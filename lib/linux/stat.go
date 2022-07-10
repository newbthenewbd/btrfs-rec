// Based on https://github.com/datawire/ocibuild/blob/master/pkg/python/stat.go

package linux

type StatMode uint32

//nolint:deadcode,varcheck // not all of these modes will be used
const (
	// 16 bits = 5⅓ octal characters

	ModeFmt StatMode = 0o17_0000 // mask for the type bits

	_ModeFmtUnused000  StatMode = 0o00_0000
	ModeFmtNamedPipe   StatMode = 0o01_0000 // type: named pipe (FIFO)
	ModeFmtCharDevice  StatMode = 0o02_0000 // type: character device
	_ModeFmtUnused003  StatMode = 0o03_0000
	ModeFmtDir         StatMode = 0o04_0000 // type: directory
	_ModeFmtUnused005  StatMode = 0o05_0000
	ModeFmtBlockDevice StatMode = 0o06_0000 // type: block device
	_ModeFmtUnused007  StatMode = 0o07_0000
	ModeFmtRegular     StatMode = 0o10_0000 // type: regular file
	_ModeFmtUnused011  StatMode = 0o11_0000
	ModeFmtSymlink     StatMode = 0o12_0000 // type: symbolic link
	_ModeFmtUnused013  StatMode = 0o13_0000
	ModeFmtSocket      StatMode = 0o14_0000 // type: socket file
	_ModeFmtUnused015  StatMode = 0o15_0000
	_ModeFmtUnused016  StatMode = 0o16_0000
	_ModeFmtUnused017  StatMode = 0o17_0000

	ModePerm StatMode = 0o00_7777 // mask for permission bits

	ModePermSetUID StatMode = 0o00_4000 // permission: set user id
	ModePermSetGID StatMode = 0o00_2000 // permission: set group ID
	ModePermSticky StatMode = 0o00_1000 // permission: sticky bit

	ModePermUsrR StatMode = 0o00_0400 // permission: user: read
	ModePermUsrW StatMode = 0o00_0200 // permission: user: write
	ModePermUsrX StatMode = 0o00_0100 // permission: user: execute

	ModePermGrpR StatMode = 0o00_0040 // permission: group: read
	ModePermGrpW StatMode = 0o00_0020 // permission: group: write
	ModePermGrpX StatMode = 0o00_0010 // permission: group: execute

	ModePermOthR StatMode = 0o00_0004 // permission: other: read
	ModePermOthW StatMode = 0o00_0002 // permission: other: write
	ModePermOthX StatMode = 0o00_0001 // permission: other: execute
)

// IsDir reports whether mode describes a directory.
//
// That is, it tests that the ModeFmt bits are set to ModeFmtDir.
func (mode StatMode) IsDir() bool {
	return mode&ModeFmt == ModeFmtDir
}

// IsRegular reports whether m describes a regular file.
//
// That is, it tests that the ModeFmt bits are set to ModeFmtRegular.
func (mode StatMode) IsRegular() bool {
	return mode&ModeFmt == ModeFmtRegular
}

// String returns a textual representation of the mode.
//
// This is the format that POSIX specifies for showing the mode in the
// output of the `ls -l` command.  POSIX does not specify the
// character to use to indicate a ModeFmtSocket file; this method uses
// 's' (GNU `ls` behavior; though POSIX notes that many
// implementations use '=' for sockets).
func (mode StatMode) String() string {
	buf := [10]byte{
		// type: This string is easy; it directly pairs with
		// the above ModeFmtXXX list above; the character in
		// the string left-to-right corresponds with the
		// constant in the list top-to-bottom.
		"?pc?d?b?-?l?s???"[mode>>12],

		// owner
		"-r"[(mode>>8)&0o1],
		"-w"[(mode>>7)&0o1],
		"-xSs"[((mode>>6)&0o1)|((mode>>10)&0o2)],

		// group
		"-r"[(mode>>5)&0o1],
		"-w"[(mode>>4)&0o1],
		"-xSs"[((mode>>3)&0o1)|((mode>>9)&0o2)],

		// group
		"-r"[(mode>>2)&0o1],
		"-w"[(mode>>1)&0o1],
		"-xTt"[((mode>>0)&0o1)|((mode>>8)&0o2)],
	}

	return string(buf[:])
}
