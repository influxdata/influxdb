package resultset

import (
	"context"

	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/models"
)

// ResultSet describes an time series iterator.
type ResultSet interface {
	// Next advances the ResultSet and returns the next cursor. It returns nil
	// when there are no more cursors.
	Next(ctx context.Context) SeriesCursor

	// Close releases any resources allocated by the ResultSet.
	Close()

	// Err returns the first error encountered by the ResultSet.
	Err() error
}

// SeriesCursor is an iterator for a single series key.
//
// A SeriesCursor is cast to a TypedCursor in order to
// enumerate the data for the SeriesKey.
type SeriesCursor interface {
	// SeriesKey returns the series key for the receiver.
	SeriesKey() models.Escaped
	// Close releases any resources used by the receiver.
	Close()
	// Err returns the last error by the receiver.
	Err() error
}

// A TypedCursor provides access to the time series data for
// a SeriesCursor.
type TypedCursor[T BlockType] interface {
	SeriesCursor
	// Next returns the next block of data for the receiver.
	// When TimeArray.Len equals 0, there is no more data.
	Next() *TimeArray[T]
}

type (
	FloatCursor    TypedCursor[float64]
	IntegerCursor  TypedCursor[int64]
	UnsignedCursor TypedCursor[uint64]
	StringCursor   TypedCursor[string]
	BooleanCursor  TypedCursor[bool]
)
