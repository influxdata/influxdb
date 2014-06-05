package datastore

import (
	"math"
)

func int64ToUint64(i int64) uint64 {
	if i < 0 {
		return uint64(i + math.MaxInt64)
	}

	return uint64(i) + uint64(math.MaxInt64)
}

func uint64ToInt64(i uint64) int64 {
	if i > uint64(math.MaxInt64) {
		return int64(i-math.MaxInt64) - int64(1)
	}

	return int64(i) - math.MaxInt64 - int64(1)
}
