// portability wrapper for Mmap/Munmap
// +build plan9

package tsm1

import (
	"golang.org/x/sys/plan9"
)

var Mmap = plan9.Mmap
var Munmap = plan9.Munmap
