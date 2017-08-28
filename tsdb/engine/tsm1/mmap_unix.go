// +build !windows,!plan9,!solaris

package tsm1

import (
	"os"
	"syscall"
)

func mmap(f *os.File, offset int64, length int) ([]byte, error) {
	// anonymous mapping
	if f == nil {
		return syscall.Mmap(-1, 0, length, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	}

	return syscall.Mmap(int(f.Fd()), 0, length, syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmap(b []byte) (err error) {
	return syscall.Munmap(b)
}
