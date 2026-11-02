package tsm1

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/require"
)

// drainTombstoneGzipWriters empties the shared pool so a test starts from a
// known state regardless of what earlier tests left behind.
func drainTombstoneGzipWriters() {
	for {
		select {
		case <-tombstoneGzipWriters:
		default:
			return
		}
	}
}

func TestTombstoner_GzipWriterPool_Overflow(t *testing.T) {
	drainTombstoneGzipWriters()
	t.Cleanup(drainTombstoneGzipWriters)

	n := cap(tombstoneGzipWriters) + 8
	dir := t.TempDir()
	tombstoners := make([]*Tombstoner, n)
	for i := range tombstoners {
		tombstoners[i] = NewTombstoner(filepath.Join(dir, fmt.Sprintf("%03d.tsm", i)), nil)
	}

	// Check out n writers concurrently held: pool is empty, so all are fresh.
	first := make(map[*gzip.Writer]struct{}, n)
	for _, ts := range tombstoners {
		require.NoError(t, ts.Add([][]byte{[]byte("a")}))
		require.NotNil(t, ts.gz)
		first[ts.gz] = struct{}{}
	}
	require.Len(t, first, n, "each pending tombstoner must own a distinct writer")
	require.Empty(t, tombstoneGzipWriters)

	// Recycle: only cap(pool) writers are retained, the rest are dropped.
	for _, ts := range tombstoners {
		require.NoError(t, ts.Flush())
		require.Nil(t, ts.gz)
	}
	require.Len(t, tombstoneGzipWriters, cap(tombstoneGzipWriters))

	// Second round: pooled writers are reused first, then fresh ones allocated.
	reused, fresh := 0, 0
	second := make(map[*gzip.Writer]struct{}, n)
	for _, ts := range tombstoners {
		require.NoError(t, ts.Add([][]byte{[]byte("b")}))
		if _, ok := first[ts.gz]; ok {
			reused++
		} else {
			fresh++
		}
		second[ts.gz] = struct{}{}
	}
	require.Len(t, second, n, "a recycled writer must never be handed to two pending tombstoners")
	require.Equal(t, cap(tombstoneGzipWriters), reused)
	require.Equal(t, n-cap(tombstoneGzipWriters), fresh)
	require.Empty(t, tombstoneGzipWriters)

	for _, ts := range tombstoners {
		require.NoError(t, ts.Flush())
	}
	require.Len(t, tombstoneGzipWriters, cap(tombstoneGzipWriters))

	// Recycled writers must still produce readable tombstones.
	for i := range tombstoners {
		var keys []string
		require.NoError(t, NewTombstoner(tombstoners[i].Path, nil).Walk(func(ts Tombstone) error {
			keys = append(keys, string(ts.Key))
			return nil
		}))
		require.Equal(t, []string{"a", "b"}, keys, "tombstoner %d", i)
	}
}
