package tsm1

import (
	"os"
	"syscall"
	"unsafe"
	"reflect"
)

// maxMapSize represents the largest mmap size supported by Bolt. 
const maxMapSize = 0xFFFFFFFFFFFF // 256TB 

// mmap implementation for Windows
// Based on: https://github.com/edsrzf/mmap-go
// Based on: https://github.com/boltdb/bolt/bolt_windows.go
// Ref: https://groups.google.com/forum/#!topic/golang-nuts/g0nLwQI9www
func mmap(f *os.File, offset int64, length int) (out []byte,err error) {
	// Open a file mapping handle.
	sizelo := uint32(length >> 32)
	sizehi := uint32(length) & 0xffffffff
	h, errno := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READONLY, sizelo, sizehi, nil)
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

	// Convert to a byte array.
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&out))
	hdr.Data = uintptr(unsafe.Pointer(addr))
	hdr.Len = length
	hdr.Cap = length

	return
}

// munmap Windows implementation
// Based on: https://github.com/edsrzf/mmap-go
// Based on: https://github.com/boltdb/bolt/bolt_windows.go
func munmap(b []byte) (err error) {
	
	addr := (uintptr)(unsafe.Pointer(&b[0]))
	if err := syscall.UnmapViewOfFile(addr); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}
