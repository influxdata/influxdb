//go:build !windows
// +build !windows

package fs

import (
	"os"
	"syscall"

	unix "golang.org/x/sys/unix"
)

// SyncDir flushes any file renames to the filesystem.
func SyncDir(dirName string) error {
	// fsync the dir to flush the rename
	dir, err := os.OpenFile(dirName, os.O_RDONLY, os.ModeDir)
	if err != nil {
		return err
	}
	defer dir.Close()

	// While we're on unix, we may be running in a Docker container that is
	// pointed at a Windows volume over samba. That doesn't support fsyncs
	// on directories. This shows itself as an EINVAL, so we ignore that
	// error.
	err = dir.Sync()
	if pe, ok := err.(*os.PathError); ok && pe.Err == syscall.EINVAL {
		err = nil
	} else if err != nil {
		return err
	}

	return dir.Close()
}

// RenameFileWithReplacement will replace any existing file at newpath with the contents
// of oldpath.
//
// If no file already exists at newpath, newpath will be created using the contents
// of oldpath. If this function returns successfully, the contents of newpath will
// be identical to oldpath, and oldpath will be removed.
func RenameFileWithReplacement(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

// RenameFile renames oldpath to newpath, returning an error if newpath already
// exists. If this function returns successfully, the contents of newpath will
// be identical to oldpath, and oldpath will be removed.
func RenameFile(oldpath, newpath string) error {
	if _, err := os.Stat(newpath); err == nil {
		return newFileExistsError(newpath)
	}

	return os.Rename(oldpath, newpath)
}

// CreateFileWithReplacement will create a new file at any path, removing the
// contents of the old file
func CreateFileWithReplacement(newpath string) (*os.File, error) {
	return os.Create(newpath)
}

// CreateFile creates a new file at newpath, returning an error if newpath already
// exists
func CreateFile(newpath string) (*os.File, error) {
	if _, err := os.Stat(newpath); err == nil {
		return nil, newFileExistsError(newpath)
	}

	return os.Create(newpath)
}

// DiskUsage returns disk usage of disk of path
func DiskUsage(path string) (*DiskStatus, error) {
	fs := unix.Statfs_t{}
	if err := unix.Statfs(path, &fs); err != nil {
		return nil, err
	}

	var disk DiskStatus
	disk.All = fs.Blocks * uint64(fs.Bsize)
	disk.Avail = uint64(fs.Bavail) * uint64(fs.Bsize)
	disk.Free = fs.Bfree * uint64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	return &disk, nil
}
