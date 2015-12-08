package tsm1

import (
	"fmt"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

var ErrMapFileLength = fmt.Errorf("mapping file length is invalid.")

func mmap(f *os.File, offset int64, length int) ([]byte, error) {
	if length <= 0 {
		return nil, ErrMapFileLength
	}

	// Open a file mapping handle.
	sizeLow := uint32(length >> 32)
	sizeHigh := uint32(length) & 0xffffffff
	h, errno := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil,
		syscall.PAGE_READONLY, sizeLow, sizeHigh, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map.
	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(length))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	// Close mapping handle.
	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	var data []byte
	slice := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	slice.Data = uintptr(unsafe.Pointer(addr))
	slice.Len = length
	slice.Cap = length
	return data, nil
}

func munmap(b []byte) (err error) {
	if nil == b {
		return nil
	}

	slice := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	if err := syscall.UnmapViewOfFile(slice.Data); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	slice.Data = uintptr(0)
	slice.Len = 0
	slice.Cap = 0
	return nil
}
