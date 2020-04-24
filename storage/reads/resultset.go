package reads

import (
	"context"
	"math"

	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/v1/models"
	"github.com/influxdata/influxdb/v2/v1/tsdb/cursors"
)

type multiShardCursors interface {
	createCursor(row SeriesRow) cursors.Cursor
	newAggregateCursor(ctx context.Context, agg *datatypes.Aggregate, cursor cursors.Cursor) cursors.Cursor
}

type resultSet struct {
	ctx          context.Context
	agg          *datatypes.Aggregate
	seriesCursor SeriesCursor
	seriesRow    SeriesRow
	arrayCursors multiShardCursors
}

func NewFilteredResultSet(ctx context.Context, req *datatypes.ReadFilterRequest, seriesCursor SeriesCursor) ResultSet {
	return &resultSet{
		ctx:          ctx,
		seriesCursor: seriesCursor,
		arrayCursors: newMultiShardArrayCursors(ctx, req.Range.Start, req.Range.End, true, math.MaxInt64),
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

	return true
}

func (r *resultSet) Cursor() cursors.Cursor {
	cur := r.arrayCursors.createCursor(r.seriesRow)
	if r.agg != nil {
		cur = r.arrayCursors.newAggregateCursor(r.ctx, r.agg, cur)
	}
	return cur
}

func (r *resultSet) Tags() models.Tags {
	return r.seriesRow.Tags
}

// Stats returns the stats for the underlying cursors.
// Available after resultset has been scanned.
func (r *resultSet) Stats() cursors.CursorStats { return r.seriesRow.Query.Stats() }
