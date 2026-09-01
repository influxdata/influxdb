//go:build !windows

package exit_test

import (
	"syscall"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/exit"
)

// TestClassifyErrno covers the POSIX table in errno_unix.go. Windows has its
// own values and its own test; nothing here is portable, which is why the two
// tables exist separately in the first place.
func TestClassifyErrno(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errno syscall.Errno
		want  int
	}{
		{syscall.EACCES, exit.CodeNoPerm},
		{syscall.EPERM, exit.CodeNoPerm},
		{syscall.ENOENT, exit.CodeNoInput},
		{syscall.ENOTDIR, exit.CodeNoInput},
		{syscall.EISDIR, exit.CodeNoInput},
		{syscall.ENOSPC, exit.CodeCantCreate},
		{syscall.EDQUOT, exit.CodeCantCreate},
		{syscall.EROFS, exit.CodeCantCreate},
		{syscall.EIO, exit.CodeIOErr},
		{syscall.EADDRINUSE, exit.CodeUnavailable},
		{syscall.EADDRNOTAVAIL, exit.CodeUnavailable},
		{syscall.ECONNREFUSED, exit.CodeUnavailable},
		{syscall.EMFILE, exit.CodeOSErr},
		{syscall.ENFILE, exit.CodeOSErr},
		{syscall.ENOMEM, exit.CodeOSErr},
	}

	for _, tt := range tests {
		t.Run(tt.errno.Error(), func(t *testing.T) {
			t.Parallel()
			requireErrnoClassifies(t, tt.errno, tt.want)
		})
	}
}
