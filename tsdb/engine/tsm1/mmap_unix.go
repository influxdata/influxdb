//go:build !windows && !plan9

package tsm1

import (
	"os"
	"syscall"

	"github.com/influxdata/influxdb/v2/pkg/file"
	"golang.org/x/sys/unix"
)

func mmap(f *os.File, offset int64, length int) ([]byte, error) {
	// anonymous mapping
	if f == nil {
		return unix.Mmap(-1, 0, length, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	}

	mmap, err := unix.Mmap(int(f.Fd()), 0, length, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	return mmap, nil
}

func munmap(b []byte) (err error) {
	return unix.Munmap(b)
}

// madviseWillNeed gives the kernel the mmap madvise value MADV_WILLNEED, hinting
// that we plan on using the provided buffer in the near future.
func madviseWillNeed(b []byte) error {
	return madvise(b, syscall.MADV_WILLNEED)
}

func madviseDontNeed(b []byte) error {
	return madvise(b, syscall.MADV_DONTNEED)
}

// From: github.com/boltdb/bolt/bolt_unix.go
func madvise(b []byte, advice int) (err error) {
	return unix.Madvise(b, advice)
}

// rename moves the backing file to path while leaving the existing memory
// mapping intact. On Unix a mapping is backed by the inode, so it survives both
// the rename of the file and the closing of any descriptor to it. This matters
// because in-flight readers hold raw slices into m.b (for example the index keys
// returned by KeyAt) without holding the FileStore lock; FileStore.replace only
// reaches this path when the file is still referenced (InUse). Keeping the
// mapping alive lets those readers finish safely, and the mapping is released
// later by close() once all references drain and the purger closes the reader.
//
// Only the descriptor is reopened at the new path so that path() and the
// eventual file removal operate on the renamed file. The mapping (m.b) and the
// index that aliases it are intentionally left untouched.
func (m *mmapAccessor) rename(path string) error {
	m.incAccess()

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := file.RenameFile(m.f.Name(), path); err != nil {
		return err
	}

	newf, err := os.Open(path)
	if err != nil {
		return err
	}

	oldf := m.f
	m.f = newf
	return oldf.Close()
}
