package rand

import (
	"math/rand"
	"sync"
)

// LockedSource is taken from the Go "math/rand" package.
// The default rand functions use a similar type under the hood, this does not introduce any additional
// locking than using the default functions.
type LockedSource struct {
	lk  sync.Mutex
	src rand.Source
}

func NewLockedSourceFromSeed(seed int64) *LockedSource {
	return &LockedSource{
		src: rand.NewSource(seed),
	}
}

func (r *LockedSource) Int63() (n int64) {
	r.lk.Lock()
	n = r.src.Int63()
	r.lk.Unlock()
	return
}

func (r *LockedSource) Seed(seed int64) {
	r.lk.Lock()
	r.src.Seed(seed)
	r.lk.Unlock()
}
