package tsm1

import (
	"errors"
	"os"
	"reflect"
	"sync"
	"syscall"
	"unsafe"

	"github.com/influxdata/influxdb/pkg/file"
)

// mmap implementation for Windows
// Based on: https://github.com/edsrzf/mmap-go
// Based on: https://github.com/boltdb/bolt/bolt_windows.go
// Ref: https://groups.google.com/forum/#!topic/golang-nuts/g0nLwQI9www

// We keep this map so that we can get back the original handle from the memory address.
var handleLock sync.Mutex
var handleMap = map[uintptr]syscall.Handle{}
var fileMap = map[uintptr]*os.File{}

func openSharedFile(f *os.File) (file *os.File, err error) {

	var access, createmode, sharemode uint32
	var sa *syscall.SecurityAttributes

	access = syscall.GENERIC_READ
	sharemode = uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE | syscall.FILE_SHARE_DELETE)
	createmode = syscall.OPEN_EXISTING
	fileName := f.Name()

	pathp, err := syscall.UTF16PtrFromString(fileName)
	if err != nil {
		return nil, err
	}

	h, e := syscall.CreateFile(pathp, access, sharemode, sa, createmode, syscall.FILE_ATTRIBUTE_NORMAL, 0)

	if e != nil {
		return nil, e
	}
	//NewFile does not add finalizer, need to close this manually
	return os.NewFile(uintptr(h), fileName), nil
}

func mmap(f *os.File, offset int64, length int) (out []byte, err error) {
	// TODO: Add support for anonymous mapping on windows
	if f == nil {
		return make([]byte, length), nil
	}

	// Open a file mapping handle.
	sizelo := uint32(length >> 32)
	sizehi := uint32(length) & 0xffffffff

	sharedHandle, errno := openSharedFile(f)
	if errno != nil {
		return nil, os.NewSyscallError("CreateFile", errno)
	}

	h, errno := syscall.CreateFileMapping(syscall.Handle(sharedHandle.Fd()), nil, syscall.PAGE_READONLY, sizelo, sizehi, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map.
	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(length))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	handleLock.Lock()
	handleMap[addr] = h
	fileMap[addr] = sharedHandle
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
	if e != nil {
		return os.NewSyscallError("CloseHandle", e)
	}

	file, ok := fileMap[addr]
	if !ok {
		// should be impossible; we would've seen the error above
		return errors.New("unknown base address")
	}
	delete(fileMap, addr)

	e = file.Close()
	if e != nil {
		return errors.New("close file" + e.Error())
	}
	return nil
}

// madviseWillNeed is unsupported on Windows.
func madviseWillNeed(b []byte) error { return nil }

// madviseDontNeed is unsupported on Windows.
func madviseDontNeed(b []byte) error { return nil }

func madvise(b []byte, advice int) error {
	// Not implemented
	return nil
}

// rename moves the backing file to path, used by FileStore.replace when the
// file is still referenced (InUse) by in-flight readers.
//
// LIMITATION: Windows cannot rename or delete a file that has an open handle or
// an active mapping, so the mapping must be unmapped and the descriptor closed
// before the rename, then remapped afterward. Unlike the Unix implementation
// (see mmap_unix.go), this cannot preserve the original mapping. In-flight
// readers that hold raw slices into the old mapping (for example the index keys
// returned by KeyAt, which alias m.b) without holding the FileStore lock will
// read freed memory and may fault. This is a pre-existing limitation of the
// Windows mmap path and was not introduced by the fast/slow lock split; it is
// documented here so the divergence from Unix is explicit. A correct Windows
// fix would have to drain references before unmapping, which would block the
// writer (holding both FileStore mutexes) for the lifetime of the slowest
// reader and defeat the purger design.
func (m *mmapAccessor) rename(path string) error {
	m.incAccess()

	m.mu.Lock()
	defer m.mu.Unlock()

	err := munmap(m.b)
	if err != nil {
		return err
	}

	if err := m.f.Close(); err != nil {
		return err
	}

	if err := file.RenameFile(m.f.Name(), path); err != nil {
		return err
	}

	m.f, err = os.Open(path)
	if err != nil {
		return err
	}

	if _, err := m.f.Seek(0, 0); err != nil {
		return err
	}

	stat, err := m.f.Stat()
	if err != nil {
		return err
	}

	m.b, err = mmap(m.f, 0, int(stat.Size()))
	if err != nil {
		return err
	}

	if m.mmapWillNeed {
		return madviseWillNeed(m.b)
	}
	return nil
}
