//go:build darwin || dragonfly || freebsd || linux || nacl || netbsd || openbsd

package mincore

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Mincore is a wrapper function for mincore(2).
func Mincore(data []byte) ([]byte, error) {
	vec := make([]byte, (int64(len(data))+int64(os.Getpagesize())-1)/int64(os.Getpagesize()))

	if ret, _, err := unix.Syscall(
		unix.SYS_MINCORE,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		uintptr(unsafe.Pointer(&vec[0]))); ret != 0 {
		return nil, err
	}
	return vec, nil
}
