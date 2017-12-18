package mmap

import (
	"os"
	"syscall"
	"unsafe"
)

// Map memory-maps a file.
func Map(path string, sz int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	// Use file size if map size is not passed in.
	// TODO(edd): test.
	// if sz == 0 {
	// }
	sz = fi.Size()
	if fi.Size() == 0 {
		return nil, nil
	}

	lo, hi := uint32(sz), uint32(sz>>32)
	fmap, err := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READONLY, hi, lo, nil)
	if err != nil {
		return nil, err
	}
	defer syscall.CloseHandle(fmap)

	ptr, err := syscall.MapViewOfFile(fmap, syscall.FILE_MAP_READ, 0, 0, uintptr(sz))
	if err != nil {
		return nil, err
	}
	data := (*[1 << 30]byte)(unsafe.Pointer(ptr))[:sz]

	return data, nil
}

// Unmap closes the memory-map.
func Unmap(data []byte) error {
	if data == nil {
		return nil
	}
	return syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&data[0])))
}
