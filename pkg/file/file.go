package file

import (
	goerrors "errors"
	"fmt"
	"io"
	"os"

	"github.com/influxdata/influxdb/pkg/errors"
)

var (
	ErrPermissionsTooOpen = goerrors.New("file permissions are too open")
	ErrNilParam           = goerrors.New("got nil parameter")
)

func copyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}

	out, err := os.Create(dst)
	if err != nil {
		return err
	}

	defer errors.Capture(&err, out.Close)()

	defer errors.Capture(&err, in.Close)()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}

	return out.Sync()
}

// MoveFileWithReplacement copies the file contents at `src` to `dst`.
// and deletes `src` on success.
//
// If the file at `dst` already exists, it will be truncated and its contents
// overwritten.
func MoveFileWithReplacement(src, dst string) error {
	if err := copyFile(src, dst); err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	return os.Remove(src)
}

// VerifyFilePermissivenessF checks if permissions on f are as restrictive
// or more restrictive than maxPerms. if not, then an error is returned.
// For security reasons, there is no VerifyFilePermissiveness functino
// that allows passing a path. This is to prevent TOCTOU (Time-of-Check-Time-of-Use)
// issues because of race conditions on checking file permissions versus
// opening the file.
func VerifyFilePermissivenessF(f *os.File, maxPerms os.FileMode) error {
	if f == nil {
		return fmt.Errorf("VerifyFilePermissivenessF: %w", ErrNilParam)
	}

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat failed for %q: %w", f.Name(), err)
	}
	return VerifyFileInfoPermissiveness(info, maxPerms, f.Name())
}

// VerifyFileInfoPermissiveness checks if permissions on info are as restrictive
// or more restrictive than maxPerms. If not, then an error is returned. path is
// only used to provide better messages. If path is empty, then info.Name() is used
// for error messages.
func VerifyFileInfoPermissiveness(info os.FileInfo, maxPerms os.FileMode, path string) error {
	if info == nil {
		return fmt.Errorf("VerifyFileInfoPermissiveness: %w", ErrNilParam)
	}
	perms := info.Mode().Perm()
	extraPerms := perms & ^maxPerms
	if extraPerms != 0 {
		if len(path) == 0 {
			path = info.Name()
		}
		return fmt.Errorf("%w: for %q, maximum is %04o (%s) but found %04o (%s); extra permissions: %04o (%s)",
			ErrPermissionsTooOpen, path, maxPerms, maxPerms, perms, perms, extraPerms, extraPerms)
	}
	return nil
}
