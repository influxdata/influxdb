package tsm1

import (
	"math/rand"
	"runtime"
	"sync/atomic"
)

// fault buffer is a by-default disabled helper to keep track of estimates of page faults
// during accesses. use the constants below to turn it on or off and benchmarks will report
// their estimates.

const (
	faultBufferEnabled      = false
	faultBufferSampleStacks = false
)

type faultBuffer struct {
	faults  uint64
	page    uint64
	b       []byte
	samples [][]uintptr
}

func (m *faultBuffer) len() uint32 { return uint32(len(m.b)) }

func (m *faultBuffer) access(start, length uint32) []byte {
	if faultBufferEnabled {
		current, page := int64(atomic.LoadUint64(&m.page)), int64(start)/4096
		if page != current && page != current+1 { // assume kernel precaches next page
			atomic.AddUint64(&m.faults, 1)
			if faultBufferSampleStacks && rand.Intn(1000) == 0 {
				var stack [256]uintptr
				n := runtime.Callers(0, stack[:])
				m.samples = append(m.samples, stack[:n:n])
			}
		}
		atomic.StoreUint64(&m.page, uint64(page))
	}

	end := m.len()
	if length > 0 {
		end = start + length
	}

	return m.b[start:end]
}
