//go:build windows
// +build windows

package tsm1

import (
	"fmt"
	"io"
	"os"
)

// copyOrLink - Windows does not permit deleting a file with open file handles, so
// instead of hard links, make temporary copies of files that can then be deleted.
func copyOrLink(oldPath, newPath string) (returnErr error) {
	rfd, err := os.Open(oldPath)
	if err != nil {
		return fmt.Errorf("error opening file for backup %s: %q", oldPath, err)
	} else {
		defer func() {
			if e := rfd.Close(); returnErr == nil && e != nil {
				returnErr = fmt.Errorf("error closing source file for backup %s: %q", oldPath, e)
			}
		}()
	}
	fi, err := rfd.Stat()
	if err != nil {
		fmt.Errorf("error collecting statistics from file for backup %s: %q", oldPath, err)
	}
	wfd, err := os.OpenFile(newPath, os.O_RDWR|os.O_CREATE, fi.Mode())
	if err != nil {
		return fmt.Errorf("error creating temporary file for backup %s:  %q", newPath, err)
	} else {
		defer func() {
			if e := wfd.Close(); returnErr == nil && e != nil {
				returnErr = fmt.Errorf("error closing temporary file for backup %s: %q", newPath, e)
			}
		}()
	}
	if _, err := io.Copy(wfd, rfd); err != nil {
		return fmt.Errorf("unable to copy file for backup from %s to %s: %q", oldPath, newPath, err)
	}
	if err := os.Chtimes(newPath, fi.ModTime(), fi.ModTime()); err != nil {
		return fmt.Errorf("unable to set modification time on temporary backup file %s: %q", newPath, err)
	}
	return nil
}
