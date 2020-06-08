package storageflux

import "sync/atomic"

func (t *table) IsDone() bool {
	return atomic.LoadInt32(&t.used) != 0
}
