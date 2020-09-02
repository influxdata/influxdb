package tsm1

import "sync"

var bufPool sync.Pool

// getBuf returns a buffer with length size from the buffer pool.
func getBuf(size int) *[]byte {
	x := bufPool.Get()
	if x == nil {
		b := make([]byte, size)
		return &b
	}
	buf := x.(*[]byte)
	if cap(*buf) < size {
		bufPool.Put(x)
		b := make([]byte, size)
		return &b
	}
	*buf = (*buf)[:size]
	return buf
}

// putBuf returns a buffer to the pool.
func putBuf(buf *[]byte) {
	bufPool.Put(buf)
}
