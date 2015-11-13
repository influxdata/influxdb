// portability wrapper for Mmap/Munmap
// +build windows

package tsm1

import (
	"golang.org/x/sys/windows"
)

var Mmap = windows.Mmap
var Munmap = windows.Munmap
