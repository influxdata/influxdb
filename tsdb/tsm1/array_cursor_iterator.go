package tsm1

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/pkg/metrics"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/platform/models"
	"github.com/influxdata/platform/tsdb"
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
