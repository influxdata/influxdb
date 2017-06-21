// +build solaris

package mmap

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func Map(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	} else if fi.Size() == 0 {
		return nil, nil
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Unmap closes the memory-map.
func Unmap(data []byte) error {
	if data == nil {
		return nil
	}
	return unix.Munmap(data)
}
