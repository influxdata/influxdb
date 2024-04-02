package fs

import (
	"errors"
	"io/fs"
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
	// Documentation/devices.txt file.

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
	// exception for tmpfs, which some distros use for /tmp.
	// Since the minor IDs are assigned dynamically, we'll find the device ID
	// for /tmp. If /tmp's device ID matches this st's, then it is safe to assume
	// the file is in tmpfs. Otherwise, it is a special device that is not tmpfs.
	// Note that if /tmp is not tmpfs, then the device IDs won't match since
	// tmp would be a standard file system.
	tmpSt, err := os.Stat(os.TempDir())
	if err != nil {
		// We got an error getting stats on /tmp, but that just means we can't
		// say the file is in tmpfs. We'll still go ahead and call it a special file.
		return true, nil
	}
	tmpDevId, err := getDevId(tmpSt)
	if err != nil {
		// See above for why we're returning an error here.
		return true, nil
	}
	// It's a special file unless the device ID matches /tmp's ID.
	return devId != tmpDevId, nil
}
