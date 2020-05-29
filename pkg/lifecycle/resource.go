package lifecycle

import "sync"

// Resource keeps track of references and has some compile time debug hooks
// to help diagnose leaks. It keeps track of if it is open or not and allows
// blocking until all references are released.
type Resource struct {
	stmu sync.Mutex     // protects state transitions
	chmu sync.RWMutex   // protects channel mutations
	ch   chan struct{}  // signals references to close
	wg   sync.WaitGroup // counts outstanding references
}

// Open marks the resource as open.
func (res *Resource) Open() {
	res.stmu.Lock()
	defer res.stmu.Unlock()

	res.chmu.Lock()
	res.ch = make(chan struct{})
	res.chmu.Unlock()
}

// Close waits for any outstanding references and marks the resource as closed
// so that Acquire returns an error.
func (res *Resource) Close() {
	res.stmu.Lock()
	defer res.stmu.Unlock()

	res.chmu.Lock()
	if res.ch != nil {
		close(res.ch) // signal any references.
		res.ch = nil  // stop future Acquires
	}
	res.chmu.Unlock()

	res.wg.Wait() // wait for any acquired references
}

// Opened returns true if the resource is currently open. It may be immediately
// false in the presence of concurrent Open and Close calls.
func (res *Resource) Opened() bool {
	res.chmu.RLock()
	opened := res.ch != nil
	res.chmu.RUnlock()

	return opened
}

// Acquire returns a Reference used to keep alive some resource.
func (res *Resource) Acquire() (*Reference, error) {
	res.chmu.RLock()
	defer res.chmu.RUnlock()

	ch := res.ch
	if ch == nil {
		return nil, resourceClosed()
	}

	res.wg.Add(1)
	return live.track(&Reference{wg: &res.wg, ch: ch}), nil
}

// Reference is an open reference for some resource.
type Reference struct {
	once sync.Once
	wg   *sync.WaitGroup
	ch   <-chan struct{}
	id   uint64
}

// Closing returns a channel that will be closed when the associated resource begins closing.
func (ref *Reference) Closing() <-chan struct{} { return ref.ch }

// Release causes the Reference to be freed. It is safe to call multiple times.
func (ref *Reference) Release() {
	ref.once.Do(func() {
		live.untrack(ref)
		ref.wg.Done()
	})
}

// Close makes a Reference an io.Closer. It is safe to call multiple times.
func (ref *Reference) Close() error {
	ref.Release()
	return nil
}

// References is a helper to aggregate a group of references.
type References []*Reference

// Release releases all of the references. It is safe to call multiple times.
func (refs References) Release() {
	for _, ref := range refs {
		ref.Release()
	}
}

// Close makes References an io.Closer. It is safe to call multiple times.
func (refs References) Close() error {
	refs.Release()
	return nil
}
