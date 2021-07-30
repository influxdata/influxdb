package storageflux

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/influxdata/flux/execute"
)

func TestTagsCache_GetBounds_Concurrency(t *testing.T) {
	// Concurrently use the tags cache by retrieving
	// a tag of random sizes and then iterating over the
	// retrieved tag. The test should exceed the cache's
	// size so we get values being evicted.
	cache := newTagsCache(4)
	bounds := execute.Bounds{
		Start: execute.Time(time.Second),
		Stop:  execute.Time(2 * time.Second),
	}
	mem := NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			for j := 0; j < 128; j++ {
				l := rand.Intn(128) + 1
				start, stop := cache.GetBounds(bounds, l, mem)
				for i := 0; i < l; i++ {
					if want, got := int64(bounds.Start), start.Value(i); want != got {
						t.Errorf("unexpected value in start array: %d != %d", want, got)
						start.Release()
						stop.Release()
						return
					}
					if want, got := int64(bounds.Stop), stop.Value(i); want != got {
						t.Errorf("unexpected value in stop array: %d != %d", want, got)
						start.Release()
						stop.Release()
						return
					}
				}
				start.Release()
				stop.Release()
			}
		}(i)
	}

	wg.Wait()
	cache.Release()
}

func TestTagsCache_GetTags_Concurrency(t *testing.T) {
	// Concurrently use the tags cache by retrieving
	// a tag of random sizes and then iterating over the
	// retrieved tag. The test should exceed the cache's
	// size so we get values being evicted.
	cache := newTagsCache(4)
	mem := NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// Cardinality of 8 so it exceeds the cache size
			// but we also reuse tags across different goroutines.
			value := fmt.Sprintf("t%d", i%8)
			for j := 0; j < 128; j++ {
				l := rand.Intn(128) + 1
				vs := cache.GetTag(value, l, mem)
				for i := 0; i < l; i++ {
					if want, got := value, vs.Value(i); want != got {
						t.Errorf("unexpected value in array: %s != %s", want, got)
						vs.Release()
						return
					}
				}
				vs.Release()
			}
		}(i)
	}

	wg.Wait()
	cache.Release()
}

type CheckedAllocator struct {
	mem *memory.CheckedAllocator
	mu  sync.Mutex
}

func NewCheckedAllocator(mem memory.Allocator) *CheckedAllocator {
	return &CheckedAllocator{
		mem: memory.NewCheckedAllocator(mem),
	}
}

func (c *CheckedAllocator) Allocate(size int) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mem.Allocate(size)
}

func (c *CheckedAllocator) Reallocate(size int, b []byte) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mem.Reallocate(size, b)
}

func (c *CheckedAllocator) Free(b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mem.Free(b)
}

func (c *CheckedAllocator) AssertSize(t memory.TestingT, sz int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mem.AssertSize(t, sz)
}
