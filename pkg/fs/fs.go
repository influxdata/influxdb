package fs

import (
	"fmt"
	"io"
	"os"
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

// MoveFileWithReplacement copies the file contents at `src` to `dst`.
//
// If the file at `dst` already exists, it will be truncated and its contents
// overwritten.
func MoveFileWithReplacement(src, dst string) error {
	out, err := os.Create(dst)
	if err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}

	defer in.Close()

	if _, err = io.Copy(out, in); err != nil {
		out.Close()
		return err
	}

	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}

	if err := out.Close(); err != nil {
		return err
	}

	return os.Remove(src)
}
