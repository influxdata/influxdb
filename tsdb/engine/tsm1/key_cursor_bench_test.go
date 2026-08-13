package tsm1_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/stretchr/testify/require"
)

// BenchmarkKeyCursor_OpenClose measures the create-and-close cycle a query
// performs once per series-field per shard.
//
// KeyCursor.Close sits on the hot path of both query engines - ten call sites in
// array_cursor.gen.go for the Flux read path, and ten in iterator.gen.go for
// InfluxQL, which is the 1.x primary read path. A guard added there is therefore
// worth measuring rather than reasoning about, even though the guard is a single
// atomic compare-and-swap.
//
// Close's existing cost is O(locations): one Unref, itself an atomic, per TSM
// file the cursor spans. The file counts below bracket how much a fixed O(1)
// guard can matter - its relative cost is largest at one file and shrinks from
// there. Creation is included because that is the real unit of work, and because
// measuring Close alone would magnify a constant against an artificially small
// denominator.
//
// No parallel variant: KeyCursors are per-query objects, so concurrent closes
// touch distinct cursors and do not contend on the guard. What a parallel
// benchmark would actually measure is FileStore.fastMu and allocator contention
// during creation, which this change does not affect.
func BenchmarkKeyCursor_OpenClose(b *testing.B) {
	for _, files := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("files=%d", files), func(b *testing.B) {
			dir := b.TempDir()

			// One TSM file per keyValues entry, all holding the same key at
			// increasing timestamps, so a cursor seeking from 0 ascending spans
			// every file and Close has `files` locations to release.
			data := make([]keyValues, files)
			for i := range data {
				data[i] = keyValues{"cpu", []tsm1.Value{tsm1.NewValue(int64(i), float64(i))}}
			}
			_, err := newFileDir(b, dir, data...)
			require.NoError(b, err)

			fs := newTestFileStore(b, dir)
			require.NoError(b, fs.Open(context.Background()))

			key := []byte("cpu")
			ctx := context.Background()

			// Confirm the fixture really produces the intended fan-out, so a
			// silently thin store cannot make this look fast. The warm-up
			// cursor also takes one create/close cycle out of the timed loop.
			require.Equal(b, files, fs.Count())
			c := fs.KeyCursor(ctx, key, 0, true)
			c.Close()

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				c := fs.KeyCursor(ctx, key, 0, true)
				c.Close()
			}
		})
	}
}
