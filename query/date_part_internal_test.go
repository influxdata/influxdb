package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDatePartMap_Value(t *testing.T) {
	// 2023-01-16T10:30:45Z — a Monday.
	ts := time.Date(2023, 1, 16, 10, 30, 45, 0, time.UTC).UnixNano()
	row := &Row{Time: ts}

	require.Equal(t, int64(2023), datePartMap{expr: Year, loc: time.UTC}.Value(row))
	require.Equal(t, int64(1), datePartMap{expr: Month, loc: time.UTC}.Value(row))
	require.Equal(t, int64(10), datePartMap{expr: Hour, loc: time.UTC}.Value(row))
	require.Equal(t, int64(1), datePartMap{expr: DOW, loc: time.UTC}.Value(row)) // Monday = 1

	// nil location is treated as UTC.
	require.Equal(t, int64(2023), datePartMap{expr: Year, loc: nil}.Value(row))

	// Non-UTC location shifts the hour. America/New_York is UTC-5 in January.
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	require.Equal(t, int64(5), datePartMap{expr: Hour, loc: ny}.Value(row)) // 10:30 UTC -> 05:30 EST
}

// TestEncodeDecodeAux_DatePartKey ensures a DecodedDatePartKey grouping value
// survives the iterator wire codec (encodeAux/decodeAux). This is the codec used
// to stream iterators between enterprise data nodes; without explicit handling the
// key serializes to null and all date_part GROUP BY buckets collapse into one.
func TestEncodeDecodeAux_DatePartKey(t *testing.T) {
	// Use a non-zero Expr (Month) so the expr byte is actually exercised, alongside
	// neighbouring aux values of other types.
	key := DecodedDatePartKey{Expr: Month, Val: 12}
	aux := []interface{}{int64(7), key, "host1"}

	got := decodeAux(encodeAux(aux))

	require.Len(t, got, 3)
	require.Equal(t, int64(7), got[0])
	require.Equal(t, key, got[1], "DecodedDatePartKey must survive the iterator wire codec")
	require.Equal(t, "host1", got[2])
}
