package table

import (
	"sync/atomic"

	"github.com/apache/arrow/go/v14/arrow/memory"
)

// trackedAllocator is a memory.Allocator that tracks the total allocation size.
type trackedAllocator struct {
	mem memory.GoAllocator
	sz  int64
}

// CurrentAlloc returns the current allocation size in bytes.
func (a *trackedAllocator) CurrentAlloc() int { return int(atomic.LoadInt64(&a.sz)) }

func (a *trackedAllocator) Allocate(size int) []byte {
	atomic.AddInt64(&a.sz, int64(size))
	return a.mem.Allocate(size)
}

func (a *trackedAllocator) Reallocate(size int, b []byte) []byte {
	atomic.AddInt64(&a.sz, int64(size-len(b)))
	return a.mem.Reallocate(size, b)
}

func (a *trackedAllocator) Free(b []byte) {
	atomic.AddInt64(&a.sz, int64(len(b)*-1))
	a.mem.Free(b)
}
