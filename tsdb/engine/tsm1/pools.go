package tsm1

import "github.com/influxdata/influxdb/pkg/pool"

var bufPool = pool.NewBytes(10)

// getBuf returns a buffer with length size from the buffer pool.
func getBuf(size int) []byte {
	return bufPool.Get(size)
}

// putBuf returns a buffer to the pool.
func putBuf(buf []byte) {
	bufPool.Put(buf)
}
