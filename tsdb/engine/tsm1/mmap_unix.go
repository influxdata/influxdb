// +build !windows,!plan9,!solaris

package tsm1

import (
	"os"
	"syscall"
)

func mmap(f *os.File, offset int64, length int) ([]byte, error) {
	mmap, err := syscall.Mmap(int(f.Fd()), 0, length, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return mmap, nil
}

func munmap(b []byte) (err error) {
	return syscall.Munmap(b)
}
