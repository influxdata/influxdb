package fs

import (
	"os"
	"syscall"
	"unsafe"
)

func SyncDir(dirName string) error {
	return nil
}

// RenameFileWithReplacement will replace any existing file at newpath with the contents
// of oldpath.
//
// If no file already exists at newpath, newpath will be created using the contents
// of oldpath. If this function returns successfully, the contents of newpath will
// be identical to oldpath, and oldpath will be removed.
func RenameFileWithReplacement(oldpath, newpath string) error {
	if _, err := os.Stat(newpath); err == nil {
		if err = os.Remove(newpath); nil != err {
			return err
		}
	}

	return os.Rename(oldpath, newpath)
}

// RenameFile renames oldpath to newpath, returning an error if newpath already
// exists. If this function returns successfully, the contents of newpath will
// be identical to oldpath, and oldpath will be removed.
func RenameFile(oldpath, newpath string) error {
	if _, err := os.Stat(newpath); err == nil {
		// os.Rename on Windows will return an error if the file exists, but it's
		// preferable to keep the errors the same across platforms.
		return newFileExistsError(newpath)
	}

	return os.Rename(oldpath, newpath)
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
	var disk DiskStatus
	h := syscall.MustLoadDLL("kernel32.dll")
	c := h.MustFindProc("GetDiskFreeSpaceExW")
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	r1, _, err := c.Call(uintptr(unsafe.Pointer(p)),
		uintptr(unsafe.Pointer(&disk.Avail)),
		uintptr(unsafe.Pointer(&disk.All)),
		uintptr(unsafe.Pointer(&disk.Free)))
	if r1 == 0 {
		return nil, err
	}
	disk.Used = disk.All - disk.Free
	return &disk, nil
}
