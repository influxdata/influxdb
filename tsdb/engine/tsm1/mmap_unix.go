// portability wrapper for Mmap/Munmap
// +build !windows,!plan9

package tsm1

import (
	"golang.org/x/sys/unix"
)

var Mmap = unix.Mmap
var Munmap = unix.Munmap
