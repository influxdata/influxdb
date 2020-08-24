package reads

import (
	"context"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

type resultSet struct {
	ctx          context.Context
	seriesCursor SeriesCursor
	seriesRow    SeriesRow
	arrayCursors *arrayCursors
	cursor       cursors.Cursor
}

func NewFilteredResultSet(ctx context.Context, req *datatypes.ReadFilterRequest, seriesCursor SeriesCursor) ResultSet {
	return &resultSet{
		ctx:          ctx,
		seriesCursor: seriesCursor,
		arrayCursors: newArrayCursors(ctx, req.Range.Start, req.Range.End, true),
	}
}

func (r *resultSet) Err() error { return nil }

// Close closes the result set. Close is idempotent.
func (r *resultSet) Close() {
	if r == nil {
		return // Nothing to do.
	}
	r.seriesRow.Query = nil
	r.seriesCursor.Close()
}

// Next returns true if there are more results available.
func (r *resultSet) Next() bool {
	if r == nil {
		return false
	}

	seriesRow := r.seriesCursor.Next()
	if seriesRow == nil {
		return false
	}
	r.seriesRow = *seriesRow
	r.cursor = r.arrayCursors.createCursor(r.seriesRow)
	return true
}

func (r *resultSet) Cursor() cursors.Cursor {
	return r.cursor
}

func (r *resultSet) Tags() models.Tags {
	return r.seriesRow.Tags
}

// Stats returns the stats for the underlying cursors.
// Available after resultset has been scanned.
func (r *resultSet) Stats() cursors.CursorStats {
	if r.seriesRow.Query == nil {
		return cursors.CursorStats{}
	}
	// All seriesRows share the same underlying cursor iterator
	// which contains the aggregated stats of the query.
	// So this seems like it is returning the stats only from the
	// last series, but this returns the stats from all series.
	return r.seriesRow.Query.Stats()
}
