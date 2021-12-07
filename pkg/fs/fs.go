package fs

import (
	"fmt"
	"io"
	"os"

	"github.com/influxdata/influxdb/v2/pkg/errors"
)

// A FileExistsError is returned when an operation cannot be completed due to a
// file already existing.
type FileExistsError struct {
	path string
}

func newFileExistsError(path string) FileExistsError {
	return FileExistsError{path: path}
}

func (e FileExistsError) Error() string {
	return fmt.Sprintf("operation not allowed, file %q exists", e.path)
}

// DiskStatus is returned by DiskUsage
type DiskStatus struct {
	All   uint64
	Used  uint64
	Free  uint64
	Avail uint64
}

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
