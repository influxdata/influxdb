//go:build !windows

package exit

import "syscall"

// errnoCodes maps the POSIX errno values influxd can plausibly reach while
// starting up to the exit status that describes them.
//
// It is deliberately not exhaustive. An errno that is not listed falls through
// to Classify's io/fs and context sentinel checks and then to CodeSoftware,
// which is the honest answer for a cause this package cannot categorize -- a
// wrong guess would be worse than a generic one, because the whole point of the
// status is to tell an operator whether retrying can help.
//
// Windows has its own table in errno_windows.go: its file and socket errors are
// Win32 and Winsock numbers, not these values.
var errnoCodes = map[syscall.Errno]int{
	// Permission. EPERM covers the privileged-port case as well as ownership.
	syscall.EACCES: CodeNoPerm,
	syscall.EPERM:  CodeNoPerm,

	// The path is missing, or is not the kind of object it was used as --
	// pointing --engine-path at a regular file yields ENOTDIR.
	syscall.ENOENT:  CodeNoInput,
	syscall.ENOTDIR: CodeNoInput,
	syscall.EISDIR:  CodeNoInput,

	// Nothing can be written or extended until an operator frees space or
	// remounts. EROFS belongs here rather than under permissions: the fix is
	// to the mount, not to the mode bits.
	syscall.ENOSPC: CodeCantCreate,
	syscall.EDQUOT: CodeCantCreate,
	syscall.EROFS:  CodeCantCreate,

	// Hardware or filesystem trouble underneath a read or write.
	syscall.EIO: CodeIOErr,

	// Something else holds the address, or the address cannot be had at all.
	syscall.EADDRINUSE:    CodeUnavailable,
	syscall.EADDRNOTAVAIL: CodeUnavailable,
	syscall.ECONNREFUSED:  CodeUnavailable,

	// Resource exhaustion in the kernel or in this process's limits.
	syscall.EMFILE: CodeOSErr,
	syscall.ENFILE: CodeOSErr,
	syscall.ENOMEM: CodeOSErr,
}
