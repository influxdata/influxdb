//go:build !linux
// +build !linux

package fs

import "io/fs"

// IsSpecialFSFromFileInfo determines if a file resides on a special file
// system (e.g. /proc, /dev/, /sys) based on its fs.FileInfo.
// The bool return value should be ignored if err is not nil.
func IsSpecialFSFromFileInfo(st fs.FileInfo) (bool, error) {
	return false, nil
}
