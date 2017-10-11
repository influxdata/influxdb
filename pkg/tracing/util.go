package tracing

import (
	"math/rand"
	"sync"
	"time"
)

var (
	seededIDGen  = rand.New(rand.NewSource(time.Now().UnixNano()))
	seededIDLock sync.Mutex
)

func randomID() (n uint64) {
	seededIDLock.Lock()
	n = uint64(seededIDGen.Int63())
	seededIDLock.Unlock()
	return
}

func randomID2() (n uint64, m uint64) {
	seededIDLock.Lock()
	n, m = uint64(seededIDGen.Int63()), uint64(seededIDGen.Int63())
	seededIDLock.Unlock()
	return
}
