package tsm1

import (
	"errors"
	"os"
	"reflect"
	"sync"
	"syscall"
	"unsafe"
)

// mmap implementation for Windows
// Based on: https://github.com/edsrzf/mmap-go
// Based on: https://github.com/boltdb/bolt/bolt_windows.go
// Ref: https://groups.google.com/forum/#!topic/golang-nuts/g0nLwQI9www

// We keep this map so that we can get back the original handle from the memory address.
var handleLock sync.Mutex
var handleMap = map[uintptr]syscall.Handle{}

func mmap(f *os.File, offset int64, length int) (out []byte, err error) {
	// Open a file mapping handle.
	sizelo := uint32(length >> 32)
	sizehi := uint32(length) & 0xffffffff
	//h, errno := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READWRITE, sizelo, sizehi, nil)
	h, errno := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READONLY, sizelo, sizehi, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map.
	//addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ | syscall.FILE_MAP_WRITE , 0, 0, uintptr(length))
	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(length))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	handleLock.Lock()
	handleMap[addr] = h
	handleLock.Unlock()

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

	handleLock.Lock()
	defer handleLock.Unlock()

	addr := (uintptr)(unsafe.Pointer(&b[0]))
	if err := syscall.UnmapViewOfFile(addr); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}

	handle, ok := handleMap[addr]
	if !ok {
		// should be impossible; we would've seen the error above
		return errors.New("unknown base address")
	}
	delete(handleMap, addr)

	e := syscall.CloseHandle(syscall.Handle(handle))
	return os.NewSyscallError("CloseHandle", e)

	return nil
}
