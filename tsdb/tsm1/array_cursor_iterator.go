package tsm1

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/metrics"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

type arrayCursorIterator struct {
	e   *Engine
	key []byte

	asc struct {
		Float    *floatArrayAscendingCursor
		Integer  *integerArrayAscendingCursor
		Unsigned *unsignedArrayAscendingCursor
		Boolean  *booleanArrayAscendingCursor
		String   *stringArrayAscendingCursor
	}

	desc struct {
		Float    *floatArrayDescendingCursor
		Integer  *integerArrayDescendingCursor
		Unsigned *unsignedArrayDescendingCursor
		Boolean  *booleanArrayDescendingCursor
		String   *stringArrayDescendingCursor
	}
}

func (q *arrayCursorIterator) Next(ctx context.Context, r *tsdb.CursorRequest) (tsdb.Cursor, error) {
	q.key = tsdb.AppendSeriesKey(q.key[:0], r.Name, r.Tags)
	id := q.e.sfile.SeriesIDTypedBySeriesKey(q.key)
	if id.IsZero() {
		return nil, nil
	}

	if grp := metrics.GroupFromContext(ctx); grp != nil {
		grp.GetCounter(numberOfRefCursorsCounter).Add(1)
	}

	var opt query.IteratorOptions
	opt.Ascending = r.Ascending
	opt.StartTime = r.StartTime
	opt.EndTime = r.EndTime

	// Return appropriate cursor based on type.
	switch typ := id.Type(); typ {
	case models.Float:
		return q.buildFloatArrayCursor(ctx, r.Name, r.Tags, r.Field, opt), nil
	case models.Integer:
		return q.buildIntegerArrayCursor(ctx, r.Name, r.Tags, r.Field, opt), nil
	case models.Unsigned:
		return q.buildUnsignedArrayCursor(ctx, r.Name, r.Tags, r.Field, opt), nil
	case models.String:
		return q.buildStringArrayCursor(ctx, r.Name, r.Tags, r.Field, opt), nil
	case models.Boolean:
		return q.buildBooleanArrayCursor(ctx, r.Name, r.Tags, r.Field, opt), nil
	default:
		panic(fmt.Sprintf("unreachable: %v", typ))
	}
}

func (q *arrayCursorIterator) seriesFieldKeyBytes(name []byte, tags models.Tags, field string) []byte {
	q.key = models.AppendMakeKey(q.key[:0], name, tags)
	q.key = append(q.key, keyFieldSeparatorBytes...)
	q.key = append(q.key, field...)
	return q.key
}

// Stats returns the cumulative stats for all cursors.
func (q *arrayCursorIterator) Stats() cursors.CursorStats {
	var stats cursors.CursorStats
	if cur := q.asc.Float; cur != nil {
		stats.Add(cur.Stats())
	} else if cur := q.asc.Integer; cur != nil {
		stats.Add(cur.Stats())
	} else if cur := q.asc.Unsigned; cur != nil {
		stats.Add(cur.Stats())
	} else if cur := q.asc.Boolean; cur != nil {
		stats.Add(cur.Stats())
	} else if cur := q.asc.String; cur != nil {
		stats.Add(cur.Stats())
	}
	if cur := q.desc.Float; cur != nil {
		stats.Add(cur.Stats())
	} else if cur := q.desc.Integer; cur != nil {
		stats.Add(cur.Stats())
	} else if cur := q.desc.Unsigned; cur != nil {
		stats.Add(cur.Stats())
	} else if cur := q.desc.Boolean; cur != nil {
		stats.Add(cur.Stats())
	} else if cur := q.desc.String; cur != nil {
		stats.Add(cur.Stats())
	}
	return stats
}
