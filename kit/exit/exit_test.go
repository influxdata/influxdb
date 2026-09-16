package exit_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/influxdata/influxdb/v2/kit/exit"
	"github.com/stretchr/testify/require"
)

// TestCodeDoesNotClassify pins the property that keeps this package's reach
// equal to its wiring: an error nobody pinned a status to exits 1, however
// classifiable its cause is. Without this, adding the package to a process's
// exit path would silently re-status every command that had not opted in.
func TestCodeDoesNotClassify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"plain", errors.New("boom")},
		{"wrapped errno", fmt.Errorf("open: %w", syscall.EACCES)},
		{"fs sentinel", fmt.Errorf("stat: %w", fs.ErrPermission)},
		{"join", errors.Join(errors.New("a"), syscall.ENOSPC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			want := exit.CodeGeneric
			if tt.err == nil {
				want = exit.CodeOK
			}
			require.Equal(t, want, exit.Code(tt.err),
				"Code must report a status only for an error that was pinned one")
		})
	}
}

func TestCodeReadsThePin(t *testing.T) {
	t.Parallel()

	base := errors.New("boom")

	t.Run("direct", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, exit.CodeConfig, exit.Code(exit.WithCode(exit.CodeConfig, base)))
	})

	t.Run("wrapped after pinning", func(t *testing.T) {
		t.Parallel()
		err := fmt.Errorf("while starting: %w", exit.WithCode(exit.CodeConfig, base))
		require.Equal(t, exit.CodeConfig, exit.Code(err),
			"a status pinned deep in a chain must survive further wrapping")
	})

	t.Run("nil is not pinned", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, exit.WithCode(exit.CodeConfig, nil),
			"WithCode must pass a nil error through, so callers need not guard")
		require.Equal(t, exit.CodeOK, exit.Code(nil))
	})
}

// TestWithCodePreservesTheError covers the two properties that let a pin be
// applied to an error already on its way to an operator: the message is
// untouched, and the chain stays walkable.
func TestWithCodePreservesTheError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("PID file exists")
	base := fmt.Errorf("writing PID file %q: %w", "/var/run/influxd.pid", sentinel)
	coded := exit.WithCode(exit.CodeUnavailable, base)

	require.Equal(t, base.Error(), coded.Error(),
		"Error must delegate verbatim: influxd prints this to stderr")
	require.ErrorIs(t, coded, sentinel, "errors.Is must still reach the wrapped sentinel")
	require.ErrorIs(t, coded, base)

	var target *fs.PathError
	require.False(t, errors.As(coded, &target), "errors.As must not invent a match")

	require.Equal(t, base, errors.Unwrap(coded))
}

// TestCodeJoinPrecedence covers the case the pin exists to make safe: influxd
// joins a startup failure with a teardown failure, and the startup arm must
// decide the status.
func TestCodeJoinPrecedence(t *testing.T) {
	t.Parallel()

	runErr := exit.WithCode(exit.CodeDataErr, errors.New("incompatible InfluxDB version"))
	shutdownErr := exit.WithCode(exit.CodeIOErr, errors.New("failed to shut down server"))

	require.Equal(t, exit.CodeDataErr, exit.Code(errors.Join(runErr, shutdownErr)),
		"the leftmost pinned status must win, so runErr leads")
	require.Equal(t, exit.CodeIOErr, exit.Code(errors.Join(nil, shutdownErr)),
		"a nil first arm must not mask the second")

	// A pinned first arm must also beat a merely classifiable second arm --
	// Code never classifies, but this states the ordering the caller relies on.
	require.Equal(t, exit.CodeDataErr,
		exit.Code(errors.Join(runErr, fmt.Errorf("close: %w", syscall.EIO))))
}

// requireErrnoClassifies runs one errno through Classify bare and through the
// two shapes it actually arrives in: wrapped by os as a *fs.PathError inside the
// launcher's own message, and as one arm of a joined error.
//
// Shared by the per-platform errno tests, which cannot be one table: Windows
// reports Win32 and Winsock numbers, so the two tables have no values in common.
func requireErrnoClassifies(t *testing.T, errno syscall.Errno, want int) {
	t.Helper()

	require.Equal(t, want, exit.Classify(errno), "expected %s", exit.Name(want))

	wrapped := fmt.Errorf("open engine: %w", &fs.PathError{
		Op:   "open",
		Path: "/var/lib/influxdb/engine",
		Err:  errno,
	})
	require.Equal(t, want, exit.Classify(wrapped),
		"Classify must reach an errno through the wrapping the launcher applies")

	require.Equal(t, want, exit.Classify(errors.Join(errors.New("other"), errno)),
		"Classify must walk a joined tree")

	// The teardown shape: Launcher.shutdownError wraps an errors.Join of one
	// error per failing closer, and an unmapped errno from an earlier closer --
	// EINVAL from a listener something else already closed -- must not end the
	// search before the arm that has a category. errors.As alone stops at the
	// first syscall.Errno it finds, which is what this pins against.
	behindUnmapped := fmt.Errorf("failed to shut down server: %w", errors.Join(
		fmt.Errorf("http: %w", syscall.EINVAL),
		fmt.Errorf("kv: %w", errno),
	))
	require.Equal(t, want, exit.Classify(behindUnmapped),
		"an unmapped errno in an earlier arm must not hide a mapped one behind it")
}

func TestClassifySentinels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want int
	}{
		{"nil", nil, exit.CodeOK},
		{"permission", fmt.Errorf("stat: %w", fs.ErrPermission), exit.CodeNoPerm},
		{"not exist", fmt.Errorf("open: %w", fs.ErrNotExist), exit.CodeNoInput},
		{"deadline", fmt.Errorf("probe: %w", context.DeadlineExceeded), exit.CodeTempFail},
		// A SIGINT during startup arrives as a canceled context. It must not
		// land on CodeSoftware, which EXIT_CODES.md tells operators to make
		// non-restartable: interrupting a slow start would leave the unit dead.
		{"canceled", fmt.Errorf("open engine: %w", context.Canceled), exit.CodeTempFail},
		{"uncategorized", errors.New("could not start task scheduler"), exit.CodeSoftware},
		{"unmapped errno", fmt.Errorf("x: %w", syscall.EINVAL), exit.CodeSoftware},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, exit.Classify(tt.err))
		})
	}
}

// TestClassifyPinWins covers the ordering that lets a site declare a category
// the OS cannot express -- "unknown store type" has no errno -- and lets the
// launcher pin once at the boundary without a later Classify second-guessing it.
func TestClassifyPinWins(t *testing.T) {
	t.Parallel()

	// A pin outranks a classifiable cause underneath it.
	err := exit.WithCode(exit.CodeConfig, fmt.Errorf("read config: %w", fs.ErrNotExist))
	require.Equal(t, exit.CodeConfig, exit.Classify(err))

	// And Classify is therefore idempotent: pinning its own result changes
	// nothing, which is what makes the launcher's single pin site safe.
	once := exit.Classify(fmt.Errorf("open: %w", syscall.EACCES))
	twice := exit.Classify(exit.WithCode(once, fmt.Errorf("open: %w", syscall.EACCES)))
	require.Equal(t, once, twice)
	require.Equal(t, exit.CodeNoPerm, twice)
}

// TestClassifyRealFileErrors runs the classifier over errors the operating
// system actually produced, rather than ones the test constructed, so the
// mapping is checked against real os package wrapping on whichever platform
// this runs.
func TestClassifyRealFileErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	notExist := filepath.Join(dir, "missing", "influxd.bolt")
	_, err := os.Open(notExist)
	require.Error(t, err)
	require.Equal(t, exit.CodeNoInput, exit.Classify(err))

	// A regular file where a directory belongs is how --engine-path is most
	// often wrong, and is what the launcher's startup-failure tests inject.
	file := filepath.Join(dir, "engine")
	require.NoError(t, os.WriteFile(file, []byte("not a directory"), 0600))
	_, err = os.Open(filepath.Join(file, "data"))
	require.Error(t, err)
	require.Equal(t, exit.CodeNoInput, exit.Classify(err))
}

func TestName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "EX_OK", exit.Name(exit.CodeOK))
	require.Equal(t, "generic", exit.Name(exit.CodeGeneric))
	require.Equal(t, "EX_NOPERM", exit.Name(exit.CodeNoPerm))
	require.Equal(t, "EX_CONFIG", exit.Name(exit.CodeConfig))
	require.Equal(t, "", exit.Name(200), "an undefined status has no sysexits name")
}

// TestCodesAreInTheUsableRange guards the reason 64-78 was chosen: every status
// influxd can exit with must avoid the ranges a shell or the kernel has already
// claimed -- 126-127 for the shell, 128-165 for signals, 255 for overflow.
func TestCodesAreInTheUsableRange(t *testing.T) {
	t.Parallel()

	codes := []int{
		exit.CodeOK, exit.CodeGeneric, exit.CodeUsage, exit.CodeDataErr,
		exit.CodeNoInput, exit.CodeNoUser, exit.CodeNoHost, exit.CodeUnavailable,
		exit.CodeSoftware, exit.CodeOSErr, exit.CodeOSFile, exit.CodeCantCreate,
		exit.CodeIOErr, exit.CodeTempFail, exit.CodeProtocol, exit.CodeNoPerm,
		exit.CodeConfig,
	}

	for _, code := range codes {
		require.GreaterOrEqual(t, code, 0)
		require.LessOrEqual(t, code, 125,
			"status %d (%s) must stay clear of the shell, signal and overflow ranges",
			code, exit.Name(code))
	}
}
