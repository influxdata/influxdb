package fs

import (
	"errors"
	"io/fs"
	"math"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// IsSpecialFSFromFileInfo determines if a file resides on a special file
// system (e.g. /proc, /dev/, /sys) based on its fs.FileInfo.
// The bool return value should be ignored if err is not nil.
func IsSpecialFSFromFileInfo(st fs.FileInfo) (bool, error) {
	// On Linux, special file systems like /proc, /dev/, and /sys are
	// considered unnamed devices (non-device mounts). These devices
	// will always have a major device number of 0 per the kernels
	// Documentation/admin-guide/devices.txt file.

	getDevId := func(st fs.FileInfo) (uint64, error) {
		st_sys_any := st.Sys()
		if st_sys_any == nil {
			return 0, errors.New("nil returned by fs.FileInfo.Sys")
		}

		st_sys, ok := st_sys_any.(*syscall.Stat_t)
		if !ok {
			return 0, errors.New("could not convert st.sys() to a *syscall.Stat_t")
		}
		return st_sys.Dev, nil
	}

	devId, err := getDevId(st)
	if err != nil {
		return false, err
	}
	if unix.Major(devId) != 0 {
		// This file is definitely not on a special file system.
		return false, nil
	}

	// We know the file is in a special file system, but we'll make an
	// exception for tmpfs, which might be used at a variety of mount points.
	// Since the minor IDs are assigned dynamically, we'll find the device ID
	// for each common tmpfs mount point, If the mount point's device ID matches this st's,
	// then it is reasonable to assume the file is in tmpfs. If the device ID
	// does not match, then st is not located in that special file system so we
	// can't give an exception based on that file system root. This check is still
	// valid even if the directory we check against isn't mounted as tmpfs, because
	// the device ID won't match so we won't grant a tmpfs exception based on it.
	// On Linux, every tmpfs mount has a different device ID, so we need to check
	// against all common ones that might be in use.
	tmpfsMounts := []string{"/tmp", "/run", "/dev/shm"}
	if tmpdir := os.TempDir(); tmpdir != "/tmp" {
		tmpfsMounts = append(tmpfsMounts, tmpdir)
	}
	if xdgRuntimeDir := os.Getenv("XDG_RUNTIME_DIR"); xdgRuntimeDir != "" {
		tmpfsMounts = append(tmpfsMounts, xdgRuntimeDir)
	}
	getFileDevId := func(n string) (uint64, error) {
		fSt, err := os.Stat(n)
		if err != nil {
			return math.MaxUint64, err
		}
		fDevId, err := getDevId(fSt)
		if err != nil {
			// See above for why we're returning an error here.
			return math.MaxUint64, nil
		}
		return fDevId, nil
	}
	for _, fn := range tmpfsMounts {
		// We ignore errors if getFileDevId fails, which could
		// mean that the file (e.g. /run) doesn't exist. The error
		if fnDevId, err := getFileDevId(fn); err == nil {
			if fnDevId == devId {
				return false, nil
			}
		}
	}

	// We didn't find any a reason to give st a special file system exception.
	return true, nil
}
