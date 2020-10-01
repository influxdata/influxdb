package fs

import (
	"fmt"
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
