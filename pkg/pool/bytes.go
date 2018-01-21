// Package pool provides pool structures to help reduce garbage collector pressure.
package pool

// Bytes is a pool of byte slices that can be re-used.  Slices in
// this pool will not be garbage collected when not in use.
type Bytes struct {
	pool chan []byte
}

// NewBytes returns a Bytes pool with capacity for max byte slices
// to be pool.
func NewBytes(max int) *Bytes {
	return &Bytes{
		pool: make(chan []byte, max),
	}
}

// Get returns a byte slice size with at least sz capacity. Items
// returned may not be in the zero state and should be reset by the
// caller.
func (p *Bytes) Get(sz int) []byte {
	var c []byte
	select {
	case c = <-p.pool:
	default:
		return make([]byte, sz)
	}

	if cap(c) < sz {
		return make([]byte, sz)
	}

	return c[:sz]
}

// Put returns a slice back to the pool.  If the pool is full, the byte
// slice is discarded.
func (p *Bytes) Put(c []byte) {
	select {
	case p.pool <- c:
	default:
	}
}

// LimitedBytes is a pool of byte slices that can be re-used.  Slices in
// this pool will not be garbage collected when not in use.  The pool will
// hold onto a fixed number of byte slices of a maximum size.  If the pool
// is empty and max pool size has not been allocated yet, it will return a
// new byte slice.  Byte slices added to the pool that are over the max size
// are dropped.
type LimitedBytes struct {
	maxSize int
	pool    chan []byte
}

// NewBytes returns a Bytes pool with capacity for max byte slices
// to be pool.
func NewLimitedBytes(capacity int, maxSize int) *LimitedBytes {
	return &LimitedBytes{
		pool:    make(chan []byte, capacity),
		maxSize: maxSize,
	}
}

// Get returns a byte slice size with at least sz capacity. Items
// returned may not be in the zero state and should be reset by the
// caller.
func (p *LimitedBytes) Get(sz int) []byte {
	var c []byte

	// If we have not allocated our capacity, return a new allocation,
	// otherwise block until one frees up.
	select {
	case c = <-p.pool:
	default:
		return make([]byte, sz)
	}

	if cap(c) < sz {
		return make([]byte, sz)
	}

	return c[:sz]
}

// Put returns a slice back to the pool.  If the pool is full, the byte
// slice is discarded.  If the byte slice is over the configured max size
// of any byte slice in the pool, it is discared.
func (p *LimitedBytes) Put(c []byte) {
	// Drop buffers that are larger than the max size
	if cap(c) >= p.maxSize {
		return
	}

	select {
	case p.pool <- c:
	default:
	}
}
