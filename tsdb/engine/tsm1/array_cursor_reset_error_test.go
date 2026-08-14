package tsm1_test

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/stretchr/testify/require"
)

// corruptTSMBlocks overwrites the block region of a TSM file with 0xFF so that
// every block's type byte is invalid and any block read fails deterministically.
// The index and footer are left intact, so the file still opens: the FileStore
// reads only the index at open time.
//
// This is deliberate fault injection against the frozen TSM layout: a 5-byte
// header (magic + version) precedes the blocks, and the footer's final 8 bytes
// hold the index offset. Corrupting the type byte rather than relying on a CRC
// mismatch matters because the array block readers skip the stored CRC
// (mmapAccessor.readFloatArrayBlock decodes from entry.Offset+4 without
// verifying it), so zeroed data could decode as an empty block with no error.
func corruptTSMBlocks(tb testing.TB, path string) {
	tb.Helper()

	b, err := os.ReadFile(path)
	require.NoError(tb, err)

	require.Greater(tb, len(b), 13, "TSM file %s too small to hold a block", path)
	indexOfs := binary.BigEndian.Uint64(b[len(b)-8:])
	require.Greater(tb, indexOfs, uint64(5), "TSM file %s has no block region", path)
	require.LessOrEqual(tb, indexOfs, uint64(len(b)-8), "TSM file %s has an invalid index offset", path)

	for i := uint64(5); i < indexOfs; i++ {
		b[i] = 0xFF
	}
	require.NoError(tb, os.WriteFile(path, b, 0666))
}

// TestArrayCursor_ResetError_ReleasesTSMReferences covers the reset-error path
// in buildXArrayCursor: reset takes ownership of a Ref'd KeyCursor before the
// block read that can fail, so on error the cursor build must release the
// KeyCursor's TSM references itself - nothing else can, since the pooled
// cursor is unreachable to the caller and CursorIterator has no teardown hook.
//
// Without that release, a corrupt block does not just fail the query: every
// TSM file the KeyCursor touched keeps a reference forever. The files can
// never be deleted and FileStore.Close parks in refsWG.Wait(), so the shard
// can neither drop nor close - with no panic or log line pointing at the
// cause. The bounded close at the end turns that hang into a reported failure.
//
// Only the float cursors are exercised: the build/reset/close stanza is
// generated identically for all five types from the same template, so per-type
// repetition would add runtime without adding coverage. Ascending and
// descending are separate pooled cursors and are both covered.
func TestArrayCursor_ResetError_ReleasesTSMReferences(t *testing.T) {
	for _, index := range tsdb.RegisteredIndexes() {
		t.Run(index, func(t *testing.T) {
			for _, tc := range []struct {
				name      string
				ascending bool
			}{
				{"ascending", true},
				{"descending", false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					e := MustOpenEngine(index)
					defer e.Close()

					require.NoError(t, e.WritePointsString(
						"cpu,host=A value=1.1 1000000000",
						"cpu,host=A value=1.2 2000000000",
					))
					e.MustWriteSnapshot()

					var tsmPaths []string
					for _, f := range e.FileStore.Files() {
						tsmPaths = append(tsmPaths, f.Path())
					}
					require.NotEmpty(t, tsmPaths, "snapshot did not produce a TSM file")

					for _, p := range tsmPaths {
						corruptTSMBlocks(t, p)
					}

					// Reopen so the FileStore maps the corrupted bytes; opening
					// succeeds because only the intact index is read.
					require.NoError(t, e.Reopen())

					// Compactions against the corrupt blocks would fail and
					// retry while holding references of their own; disable them
					// so the only reference traffic is the cursor build's.
					e.SetCompactionsEnabled(false)

					iter, err := e.CreateCursorIterator(context.Background())
					require.NoError(t, err)

					cur, err := iter.Next(context.Background(), &tsdb.CursorRequest{
						Name:      []byte("cpu"),
						Tags:      models.NewTags(map[string]string{"host": "A"}),
						Field:     "value",
						Ascending: tc.ascending,
						StartTime: math.MinInt64,
						EndTime:   math.MaxInt64,
					})
					require.Error(t, err, "reading a corrupt block must fail the cursor build")
					require.Nil(t, cur)

					var inUse []string
					for _, f := range e.FileStore.Files() {
						if f.InUse() {
							inUse = append(inUse, f.Path())
						}
					}

					// A leaked reference parks FileStore.Close in refsWG.Wait()
					// forever. Close the FileStore - not the whole engine, whose
					// fieldset does not tolerate the harness cleanup's second
					// Close - with a deadline. This must happen before any
					// assertion can fail the test: FileStore.Close nils f.files
					// before waiting, so once it has run (even hung), the
					// deferred engine Close is a no-op on the FileStore and the
					// binary exits with a reported failure instead of wedging in
					// cleanup on the leaked references. The error is asserted on
					// the test goroutine; the buffered channel carries it out of
					// the closing goroutine.
					var closeHung bool
					closeErr := make(chan error, 1)
					go func() {
						closeErr <- e.FileStore.Close()
					}()
					select {
					case err := <-closeErr:
						require.NoError(t, err)
					case <-time.After(20 * time.Second):
						closeHung = true
					}

					require.Empty(t, inUse, "failed cursor build leaked TSM references")
					require.False(t, closeHung, "FileStore close hung: leaked TSM references parked it in refsWG.Wait()")
				})
			}
		})
	}
}
