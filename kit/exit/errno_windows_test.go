package exit_test

import (
	"syscall"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/exit"
	"golang.org/x/sys/windows"
)

// TestClassifyErrno covers the Win32 and Winsock table in errno_windows.go.
//
// It exists because the POSIX values the rest of influxd's platforms report are
// not what Windows produces: a busy listen socket here is WSAEADDRINUSE
// (10048), which shares no value with syscall.EADDRINUSE. Without a table and a
// test of its own, the most common startup failure an operator hits on Windows
// would silently exit EX_SOFTWARE.
func TestClassifyErrno(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		errno syscall.Errno
		want  int
	}{
		{"ERROR_ACCESS_DENIED", windows.ERROR_ACCESS_DENIED, exit.CodeNoPerm},
		{"ERROR_WRITE_PROTECT", windows.ERROR_WRITE_PROTECT, exit.CodeNoPerm},
		{"ERROR_PRIVILEGE_NOT_HELD", windows.ERROR_PRIVILEGE_NOT_HELD, exit.CodeNoPerm},
		{"WSAEACCES", windows.WSAEACCES, exit.CodeNoPerm},

		{"ERROR_FILE_NOT_FOUND", windows.ERROR_FILE_NOT_FOUND, exit.CodeNoInput},
		{"ERROR_PATH_NOT_FOUND", windows.ERROR_PATH_NOT_FOUND, exit.CodeNoInput},
		{"ERROR_INVALID_DRIVE", windows.ERROR_INVALID_DRIVE, exit.CodeNoInput},
		{"ERROR_DIRECTORY", windows.ERROR_DIRECTORY, exit.CodeNoInput},

		{"ERROR_DISK_FULL", windows.ERROR_DISK_FULL, exit.CodeCantCreate},
		{"ERROR_HANDLE_DISK_FULL", windows.ERROR_HANDLE_DISK_FULL, exit.CodeCantCreate},

		{"ERROR_CRC", windows.ERROR_CRC, exit.CodeIOErr},
		{"ERROR_WRITE_FAULT", windows.ERROR_WRITE_FAULT, exit.CodeIOErr},
		{"ERROR_READ_FAULT", windows.ERROR_READ_FAULT, exit.CodeIOErr},
		{"ERROR_GEN_FAILURE", windows.ERROR_GEN_FAILURE, exit.CodeIOErr},

		{"WSAEADDRINUSE", windows.WSAEADDRINUSE, exit.CodeUnavailable},
		{"WSAEADDRNOTAVAIL", windows.WSAEADDRNOTAVAIL, exit.CodeUnavailable},
		{"WSAECONNREFUSED", windows.WSAECONNREFUSED, exit.CodeUnavailable},

		{"ERROR_TOO_MANY_OPEN_FILES", windows.ERROR_TOO_MANY_OPEN_FILES, exit.CodeOSErr},
		{"ERROR_NOT_ENOUGH_MEMORY", windows.ERROR_NOT_ENOUGH_MEMORY, exit.CodeOSErr},
		{"ERROR_OUTOFMEMORY", windows.ERROR_OUTOFMEMORY, exit.CodeOSErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			requireErrnoClassifies(t, tt.errno, tt.want)
		})
	}
}
