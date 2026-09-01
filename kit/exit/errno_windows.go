package exit

import (
	"syscall"

	"golang.org/x/sys/windows"
)

// errnoCodes maps the Win32 and Winsock error values influxd can plausibly
// reach while starting up to the exit status that describes them.
//
// This table exists because Windows does not report the POSIX errno values in
// errno_unix.go. A socket already in use is WSAEADDRINUSE (10048), not
// syscall.EADDRINUSE, so a single shared table would silently miss the
// busy-port case here -- the one an operator hits most often.
//
// The x/sys/windows constants are typed syscall.Errno, so they key the same map
// type the rest of the package uses. Only the values whose category is
// unambiguous are listed; anything else falls through to Classify's io/fs
// sentinel checks, which the standard library maps for Windows, and then to
// CodeSoftware.
var errnoCodes = map[syscall.Errno]int{
	// Permission, for files and for binding a reserved address.
	windows.ERROR_ACCESS_DENIED:      CodeNoPerm,
	windows.ERROR_WRITE_PROTECT:      CodeNoPerm,
	windows.ERROR_PRIVILEGE_NOT_HELD: CodeNoPerm,
	windows.WSAEACCES:                CodeNoPerm,

	// The path is missing, or is not the kind of object it was used as.
	windows.ERROR_FILE_NOT_FOUND: CodeNoInput,
	windows.ERROR_PATH_NOT_FOUND: CodeNoInput,
	windows.ERROR_INVALID_DRIVE:  CodeNoInput,
	windows.ERROR_DIRECTORY:      CodeNoInput,

	// Nothing more can be written until space is freed.
	windows.ERROR_DISK_FULL:        CodeCantCreate,
	windows.ERROR_HANDLE_DISK_FULL: CodeCantCreate,

	// Hardware or filesystem trouble underneath a read or write.
	windows.ERROR_CRC:         CodeIOErr,
	windows.ERROR_WRITE_FAULT: CodeIOErr,
	windows.ERROR_READ_FAULT:  CodeIOErr,
	windows.ERROR_GEN_FAILURE: CodeIOErr,

	// Something else holds the address, or the address cannot be had at all.
	windows.WSAEADDRINUSE:    CodeUnavailable,
	windows.WSAEADDRNOTAVAIL: CodeUnavailable,
	windows.WSAECONNREFUSED:  CodeUnavailable,

	// Resource exhaustion.
	windows.ERROR_TOO_MANY_OPEN_FILES: CodeOSErr,
	windows.ERROR_NOT_ENOUGH_MEMORY:   CodeOSErr,
	windows.ERROR_OUTOFMEMORY:         CodeOSErr,
}
