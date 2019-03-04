package lifecycle

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// Resource keeps track of references and has some compile time debug hooks
// to help diagnose leaks. It keeps track of if it is open or not and allows
// blocking until all references are released.
type Resource struct {
	sem  sync.RWMutex
	open bool
}

// Open waits for any outstanding references, of which there should be none
// and marks the reference counter as open.
func (res *Resource) Open() {
	res.sem.Lock()
	res.open = true
	res.sem.Unlock()
}

// Close waits for any outstanding references and marks the reference counter
// as closed, so that Acquire returns an error.
func (res *Resource) Close() {
	res.sem.Lock()
	res.open = false
	res.sem.Unlock()
}

// Opened returns true if the resource is currently open.
func (res *Resource) Opened() bool {
	res.sem.RLock()
	open := res.open
	res.sem.RUnlock()
	return open
}

// Acquire returns a Reference used to keep alive some resource.
func (res *Resource) Acquire() (*Reference, error) {
	res.sem.RLock()
	if !res.open {
		res.sem.RUnlock()
		return nil, resourceClosed()
	}
	// RLock intentionally left open.

	return live.track(&Reference{
		res: res,
		id:  0, // required because staticcheck
	}), nil
}

// Reference is an open reference for some resource.
type Reference struct {
	res *Resource
	id  uint64
}

// Release causes the Reference to be freed. It is safe to call multiple times.
func (r *Reference) Release() {
	// Inline a sync.Once using the res pointer as the flag for if we have
	// called unlock or not. This reduces the size of a ref.
	addr := (*unsafe.Pointer)(unsafe.Pointer(&r.res))
	old := atomic.LoadPointer(addr)
	if old != nil && atomic.CompareAndSwapPointer(addr, old, nil) {
		live.untrack(r)
		(*Resource)(old).sem.RUnlock()
	}
}

// Close makes a Reference an io.Closer. It is safe to call multiple times.
func (r *Reference) Close() error {
	r.Release()
	return nil
}

// References is a helper to aggregate a group of references.
type References []*Reference

// Release releases all of the references. It is safe to call multiple times.
func (rs References) Release() {
	for _, r := range rs {
		r.Release()
	}
}

// Close makes references an io.Closer. It is safe to call multiple times.
func (rs References) Close() error {
	rs.Release()
	return nil
}
