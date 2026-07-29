package tsm1_test

import (
	"context"
	"sync"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

// TestKeyCursor_ConcurrentClose reproduces the "sync: negative WaitGroup counter"
// panic seen in EAR-7019 and EAR-6049.
//
// KeyCursor.Close releases a reference for every location in c.seeks and only
// afterwards clears the slice:
//
//	for _, f := range c.seeks {
//		f.r.Unref()
//	}
//	c.seeks = nil
//
// The read and the clear are not atomic, so two goroutines closing the same
// cursor can both observe a populated c.seeks and both run the release loop.
// Every TSMReader involved is then Unref'd twice. That drives its reference
// count negative and permanently poisons the WaitGroup guarding its lifetime,
// after which *every* subsequent Ref or Unref on that reader panics for the
// remaining life of the process -- which is why a single occurrence produced
// days of continuous query failures.
//
// floatArrayAscendingCursor.Close has the same non-atomic check-then-clear on
// c.tsm.keyCursor, so it does not prevent the second entry either.
//
// Concurrent Close is reachable in production. storage/flux/reader.go abandons
// a table when the query context is cancelled:
//
//	case <-gi.ctx.Done():
//		table.Cancel()
//		break READ
//
// table.Cancel only sets an atomic flag; it does not interrupt an advance()
// already in flight on the Flux dispatcher goroutine. Deferred cleanup then
// calls table.Close() -> cur.Close() on the storage goroutine while the
// dispatcher is still inside advance() -> cursor.Next() -> nextArrayCursor(),
// whose first action is to close the same cursor chain.
//
// Run with -race to also surface the underlying data race on c.seeks.
func TestKeyCursor_ConcurrentClose(t *testing.T) {
	// Several TSM files for the same key so that each cursor holds several
	// locations, matching the production shape where one bad close corrupts
	// the reference counts of multiple readers at once.
	data := []keyValues{
		{"cpu", []tsm1.Value{tsm1.NewValue(0, 1.0)}},
		{"cpu", []tsm1.Value{tsm1.NewValue(1, 2.0)}},
		{"cpu", []tsm1.Value{tsm1.NewValue(2, 3.0)}},
		{"cpu", []tsm1.Value{tsm1.NewValue(3, 4.0)}},
	}

	dir := t.TempDir()
	_, err := newFileDir(t, dir, data...)
	require.NoError(t, err)

	// Deliberately not newTestFileStore: once a reader's WaitGroup has gone
	// negative, TSMReader.Close blocks in refsWG.Wait() forever. Registering a
	// cleanup that closes the FileStore would hang the test run instead of
	// failing it, so the store is closed explicitly only on success.
	fs := tsm1.NewFileStore(dir, tsdb.EngineTags{})
	require.NoError(t, fs.Open(context.Background()))

	// The race window is narrow, so try repeatedly with a fresh cursor each
	// time. In practice the detector or the panic fires well inside this.
	const (
		iterations = 500
		closers    = 2
	)

	panics := make(chan any, iterations*closers)

	for i := 0; i < iterations; i++ {
		c := fs.KeyCursor(context.Background(), []byte("cpu"), 0, true)

		var wg sync.WaitGroup
		start := make(chan struct{})

		for j := 0; j < closers; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						panics <- r
					}
				}()
				<-start // release both goroutines together
				c.Close()
			}()
		}

		close(start)
		wg.Wait()

		if len(panics) > 0 {
			break
		}
	}

	close(panics)

	if r, ok := <-panics; ok {
		// The FileStore is now unusable; do not attempt to close it.
		t.Fatalf("concurrent KeyCursor.Close corrupted a TSMReader reference count: %v", r)
	}

	require.NoError(t, fs.Close())
}
