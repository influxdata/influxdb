package fs

import (
	"errors"
	"io/fs"
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

	st_sys_any := st.Sys()
	if st_sys_any == nil {
		return false, errors.New("nil returned by fs.FileInfo.Sys")
	}

	st_sys, ok := st_sys_any.(*syscall.Stat_t)
	if !ok {
		return false, errors.New("could not convert st.sys() to a *syscall.Stat_t")
	}
	return unix.Major(st_sys.Dev) == 0, nil
}
