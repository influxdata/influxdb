package mincore

import (
	"context"
	"os"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/time/rate"
)

// Limiter defaults.
const (
	DefaultUpdateInterval = 10 * time.Second
)

// Limiter represents a token bucket rate limiter based on
type Limiter struct {
	mu         sync.Mutex
	underlying *rate.Limiter
	data       []byte    // mmap reference
	incore     []byte    // in-core vector
	updatedAt  time.Time // last incore update

	// Frequency of updates of the in-core vector.
	// Updates are performed lazily so this is the maximum frequency.
	UpdateInterval time.Duration

	// OS mincore() function.
	Mincore func(data []byte) ([]byte, error)
}

// NewLimiter returns a new instance of Limiter associated with an mmap.
// The underlying limiter can be shared to limit faults across the entire process.
func NewLimiter(underlying *rate.Limiter, data []byte) *Limiter {
	if underlying == nil {
		return nil
	}

	return &Limiter{
		underlying: underlying,
		data:       data,

		UpdateInterval: DefaultUpdateInterval,
		Mincore:        Mincore,
	}
}

// WaitPointer checks if ptr would cause a page fault and, if so, rate limits its access.
// Once a page access is limited, it's updated to be considered memory resident.
func (l *Limiter) WaitPointer(ctx context.Context, ptr unsafe.Pointer) error {
	// Check if the page is in-memory under lock.
	// However, we want to exclude the wait from the limiter lock.
	if wait, err := func() (bool, error) {
		l.mu.Lock()
		defer l.mu.Unlock()

		// Update incore mapping if data is too stale.
		if err := l.checkUpdate(); err != nil {
			return false, err
		}

		return l.wait(uintptr(ptr)), nil
	}(); err != nil {
		return err
	} else if !wait {
		return nil
	}

	return l.underlying.Wait(ctx)
}

// WaitRange checks all pages in b for page faults and, if so, rate limits their access.
// Once a page access is limited, it's updated to be considered memory resident.
func (l *Limiter) WaitRange(ctx context.Context, b []byte) error {
	// Empty byte slices will never access memory so skip them.
	if len(b) == 0 {
		return nil
	}

	// Check every page for being in-memory under lock.
	// However, we want to exclude the wait from the limiter lock.
	var n int
	if err := func() error {
		l.mu.Lock()
		defer l.mu.Unlock()

		// Update incore mapping if data is too stale.
		if err := l.checkUpdate(); err != nil {
			return err
		}

		// Iterate over every page within the range.
		pageSize := uintptr(os.Getpagesize())
		start := (uintptr(unsafe.Pointer(&b[0])) / pageSize) * pageSize
		end := (uintptr(unsafe.Pointer(&b[len(b)-1])) / pageSize) * pageSize

		for i := start; i <= end; i += pageSize {
			if l.wait(i) {
				n++
			}
		}

		return nil
	}(); err != nil {
		return err
	} else if n == 0 {
		return nil
	}

	for i := 0; i < n; i++ {
		if err := l.underlying.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (l *Limiter) wait(ptr uintptr) bool {
	// Check if page access requires page fault. If not, exit immediately.
	// If so, mark the page as memory resident afterward.
	if l.isInCore(ptr) {
		return false
	}

	// Otherwise mark page as resident in memory and rate limit.
	if i := l.index(ptr); i < len(l.incore) {
		l.incore[l.index(ptr)] |= 1
	}
	return true
}

// IsInCore returns true if the address is resident in memory or if the
// address is outside the range of the data the limiter is tracking.
func (l *Limiter) IsInCore(ptr uintptr) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.isInCore(ptr)
}

func (l *Limiter) isInCore(ptr uintptr) bool {
	if i := l.index(ptr); i < len(l.incore) {
		return (l.incore[i] & 1) == 1
	}
	return true
}

// Update updates the vector of in-core pages. Automatically updated when calling Wait().
func (l *Limiter) Update() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.update()
}

func (l *Limiter) update() error {
	vec, err := l.Mincore(l.data)
	if err != nil {
		return err
	}

	l.incore = vec
	l.updatedAt = time.Now()

	return nil
}

// checkUpdate performs an update if one hasn't been done before or the interval has passed.
func (l *Limiter) checkUpdate() error {
	if l.incore != nil && time.Since(l.updatedAt) < l.UpdateInterval {
		return nil
	}
	return l.update()
}

// index returns the position in the in-core vector that represents ptr.
func (l *Limiter) index(ptr uintptr) int {
	return int(int64(ptr-uintptr(unsafe.Pointer(&l.data[0]))) / int64(os.Getpagesize()))
}
