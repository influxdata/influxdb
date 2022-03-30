package control

import (
	"errors"
	"math"
	"sync/atomic"

	"github.com/influxdata/flux/memory"
)

type memoryManager struct {
	// initialBytesQuotaPerQuery is the initial amount of memory
	// allocated for each query. It does not count against the
	// memory pool.
	initialBytesQuotaPerQuery int64

	// memoryBytesQuotaPerQuery is the maximum amount of memory
	// that may be allocated to each query.
	memoryBytesQuotaPerQuery int64

	// unusedMemoryBytes is the amount of memory that may be used
	// when a query requests more memory. This value is only used
	// when unlimited is set to false.
	unusedMemoryBytes int64

	// unlimited indicates that the memory manager should indicate
	// there is an unlimited amount of free memory available.
	unlimited bool
}

func (m *memoryManager) getUnusedMemoryBytes() int64 {
	return atomic.LoadInt64(&m.unusedMemoryBytes)
}

func (m *memoryManager) trySetUnusedMemoryBytes(old, new int64) bool {
	return atomic.CompareAndSwapInt64(&m.unusedMemoryBytes, old, new)
}

func (m *memoryManager) addUnusedMemoryBytes(amount int64) int64 {
	return atomic.AddInt64(&m.unusedMemoryBytes, amount)
}

// createAllocator will construct an allocator and memory manager
// for the given query.
func (c *Controller) createAllocator(q *Query) {
	q.memoryManager = &queryMemoryManager{
		m:     c.memory,
		limit: c.memory.initialBytesQuotaPerQuery,
	}
	q.alloc = &memory.ResourceAllocator{
		// Use an anonymous function to ensure the value is copied.
		Limit:   func(v int64) *int64 { return &v }(q.memoryManager.limit),
		Manager: q.memoryManager,
	}
}

// queryMemoryManager is a memory manager for a specific query.
type queryMemoryManager struct {
	m     *memoryManager
	limit int64
	given int64
}

// RequestMemory will determine if the query can be given more memory
// when it is requested.
//
// Note: This function accesses the memoryManager whose attributes
// may be modified concurrently. Atomic operations are used to keep
// it lockless. The data associated with this specific query are only
// invoked from within a lock so they are safe to modify.
// Second Note: The errors here are discarded anyway so don't worry
// too much about the specific message or structure.
func (q *queryMemoryManager) RequestMemory(want int64) (got int64, err error) {
	// It can be determined statically if we are going to violate
	// the memoryBytesQuotaPerQuery.
	if q.limit+want > q.m.memoryBytesQuotaPerQuery {
		return 0, errors.New("query hit hard limit")
	}

	for {
		unused := int64(math.MaxInt64)
		if !q.m.unlimited {
			unused = q.m.getUnusedMemoryBytes()
			if unused < want {
				// We do not have the capacity for this query to
				// be given more memory.
				return 0, errors.New("not enough capacity")
			}
		}

		// The memory allocator will only request the bare amount of
		// memory it needs, but it will probably ask for more memory
		// so, if possible, give it more so it isn't repeatedly calling
		// this method.
		given := q.giveMemory(want, unused)

		// Reserve this memory for our own use.
		if !q.m.unlimited {
			if !q.m.trySetUnusedMemoryBytes(unused, unused-given) {
				// The unused value has changed so someone may have taken
				// the memory that we wanted. Retry.
				continue
			}
		}

		// Successfully reserved the memory so update our own internal
		// counter for the limit.
		q.limit += given
		q.given += given
		return given, nil
	}
}

// giveMemory will determine an appropriate amount of memory to give
// a query based on what it wants and how much it has allocated in
// the past. It will always return a number greater than or equal
// to want.
func (q *queryMemoryManager) giveMemory(want, unused int64) int64 {
	// If we can safely double the limit, then just do that.
	if q.limit > want && q.limit < unused {
		if q.limit*2 <= q.m.memoryBytesQuotaPerQuery {
			return q.limit
		}
		// Doubling the limit sends us over the quota.
		// Determine what would be our maximum amount.
		max := q.m.memoryBytesQuotaPerQuery - q.limit
		if max > want {
			return max
		}
	}

	// If we can't double because there isn't enough space
	// in unused, maybe we can just use everything.
	if unused > want && unused < q.limit {
		return unused
	}

	// Otherwise we have already determined we can give the
	// wanted number of bytes so just give that.
	return want
}

func (q *queryMemoryManager) FreeMemory(bytes int64) {
	// Not implemented. There is no problem with invoking
	// this method, but the controller won't recognize that
	// the memory has been declared as returned.
}

// Release will release all of the allocated memory to the
// memory manager.
func (q *queryMemoryManager) Release() {
	if !q.m.unlimited {
		q.m.addUnusedMemoryBytes(q.given)
	}
	q.limit = q.m.initialBytesQuotaPerQuery
	q.given = 0
}
