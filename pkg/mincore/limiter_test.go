package mincore_test

import (
	"context"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/influxdata/influxdb/v2/pkg/mincore"
	"golang.org/x/time/rate"
)

func TestLimiter(t *testing.T) {
	pageSize := os.Getpagesize()

	// Ensure limiter waits long enough between faults
	t.Run("WaitPointer", func(t *testing.T) {
		t.Parallel()

		data := make([]byte, pageSize*2)
		l := mincore.NewLimiter(rate.NewLimiter(1, 1), data) // 1 fault per sec
		l.Mincore = func(data []byte) ([]byte, error) { return make([]byte, 2), nil }

		start := time.Now()
		if err := l.WaitPointer(context.Background(), unsafe.Pointer(&data[0])); err != nil {
			t.Fatal(err)
		} else if err := l.WaitPointer(context.Background(), unsafe.Pointer(&data[pageSize])); err != nil {
			t.Fatal(err)
		}

		if d := time.Since(start); d < time.Second {
			t.Fatalf("not enough time elapsed: %s", d)
		}
	})

	// Ensure limiter waits long enough between faults for a byte slice.
	t.Run("WaitRange", func(t *testing.T) {
		t.Parallel()

		data := make([]byte, 2*pageSize)
		l := mincore.NewLimiter(rate.NewLimiter(1, 1), data) // 1 fault per sec
		l.Mincore = func(data []byte) ([]byte, error) { return make([]byte, 2), nil }

		start := time.Now()
		if err := l.WaitRange(context.Background(), data); err != nil {
			t.Fatal(err)
		}

		if d := time.Since(start); d < time.Second {
			t.Fatalf("not enough time elapsed: %s", d)
		}
	})

	// Ensure pages are marked as in-core after calling Wait() on them.
	t.Run("MoveToInMemoryAfterUse", func(t *testing.T) {
		t.Parallel()

		data := make([]byte, pageSize*10)
		l := mincore.NewLimiter(rate.NewLimiter(1, 1), data)
		l.Mincore = func(data []byte) ([]byte, error) {
			return make([]byte, 10), nil
		}
		if err := l.Update(); err != nil {
			t.Fatal(err)
		} else if l.IsInCore(uintptr(unsafe.Pointer(&data[0]))) {
			t.Fatal("expected page to not be in-memory")
		}

		if err := l.WaitPointer(context.Background(), unsafe.Pointer(&data[0])); err != nil {
			t.Fatal(err)
		} else if !l.IsInCore(uintptr(unsafe.Pointer(&data[0]))) {
			t.Fatal("expected page to be in-memory")
		}
	})

	// Ensure fresh in-core data is pulled after the update interval.
	t.Run("UpdateAfterInterval", func(t *testing.T) {
		t.Parallel()

		data := make([]byte, pageSize*10)
		l := mincore.NewLimiter(rate.NewLimiter(1, 1), data)
		l.UpdateInterval = 100 * time.Millisecond

		var n int
		l.Mincore = func(data []byte) ([]byte, error) {
			n++
			return make([]byte, 10), nil
		}

		// Wait for two pages to pull them in-memory.
		if err := l.WaitPointer(context.Background(), unsafe.Pointer(&data[0])); err != nil {
			t.Fatal(err)
		} else if err := l.WaitPointer(context.Background(), unsafe.Pointer(&data[pageSize])); err != nil {
			t.Fatal(err)
		} else if !l.IsInCore(uintptr(unsafe.Pointer(&data[0]))) {
			t.Fatal("expected page to be in-memory")
		} else if !l.IsInCore(uintptr(unsafe.Pointer(&data[pageSize]))) {
			t.Fatal("expected page to be in-memory")
		}

		// Wait for interval to pass.
		time.Sleep(l.UpdateInterval)

		// Fetch one of the previous pages and ensure the other one has been flushed from the update.
		if err := l.WaitPointer(context.Background(), unsafe.Pointer(&data[0])); err != nil {
			t.Fatal(err)
		} else if !l.IsInCore(uintptr(unsafe.Pointer(&data[0]))) {
			t.Fatal("expected page to be in-memory")
		} else if l.IsInCore(uintptr(unsafe.Pointer(&data[pageSize]))) {
			t.Fatal("expected page to not be in-memory")
		}

		if got, want := n, 2; got != want {
			t.Fatalf("refreshed %d times, expected %d times", got, want)
		}
	})

	// Ensure referencing data outside the limiter's data shows as in-memory.
	t.Run("OutOfBounds", func(t *testing.T) {
		l := mincore.NewLimiter(rate.NewLimiter(1, 1), make([]byte, pageSize))
		l.Mincore = func(data []byte) ([]byte, error) {
			return make([]byte, 1), nil
		}

		data := make([]byte, pageSize)
		if !l.IsInCore(uintptr(unsafe.Pointer(&data[0]))) {
			t.Fatal("expected out-of-bounds page to be resident")
		}
	})
}
