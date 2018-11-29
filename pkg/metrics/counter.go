package metrics

import (
	"strconv"
	"sync/atomic"
)

// The Counter type represents a numeric counter that is safe to use from concurrent goroutines.
type Counter struct {
	val  int64
	desc *desc
}

// Name identifies the name of the counter.
func (c *Counter) Name() string { return c.desc.Name }

// Value atomically returns the current value of the counter.
func (c *Counter) Value() int64 { return atomic.LoadInt64(&c.val) }

// Add atomically adds d to the counter.
func (c *Counter) Add(d int64) { atomic.AddInt64(&c.val, d) }

// String returns a string representation using the name and value of the counter.
func (c *Counter) String() string {
	var buf [16]byte
	v := strconv.AppendInt(buf[:0], c.val, 10)
	return c.desc.Name + ": " + string(v)
}
