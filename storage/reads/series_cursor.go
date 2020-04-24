package reads

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/storage"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/v1/tsdb/cursors"
	"github.com/influxdata/influxql"
)

type SeriesCursor interface {
	Close()
	Next() *SeriesRow
	Err() error
}

type SeriesRow struct {
	SortKey    []byte
	Name       []byte      // measurement name
	SeriesTags models.Tags // unmodified series tags
	Tags       models.Tags // SeriesTags with field key renamed from \xff to _field and measurement key renamed from \x00 to _measurement
	Field      string
	Query      cursors.CursorIterator
	ValueCond  influxql.Expr
}

var (
	fieldKeyBytes       = []byte(datatypes.FieldKey)
	measurementKeyBytes = []byte(datatypes.MeasurementKey)
)

type indexSeriesCursor struct {
	sqry         storage.SeriesCursor
	err          error
	cond         influxql.Expr
	seriesRow    SeriesRow
	eof          bool
	hasValueExpr bool
}

func NewIndexSeriesCursor(ctx context.Context, orgID, bucketID influxdb.ID, predicate *datatypes.Predicate, viewer Viewer) (SeriesCursor, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	cursorIterator, err := viewer.CreateCursorIterator(ctx)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	if cursorIterator == nil {
		return nil, nil
	}

	opt := query.IteratorOptions{
		Aux:        []influxql.VarRef{{Val: "key"}},
		Authorizer: query.OpenAuthorizer,
		Ascending:  true,
		Ordered:    true,
	}
	p := &indexSeriesCursor{seriesRow: SeriesRow{Query: cursorIterator}}

	if root := predicate.GetRoot(); root != nil {
		if p.cond, err = NodeToExpr(root, nil); err != nil {
			return nil, tracing.LogError(span, err)
		}

		p.hasValueExpr = HasFieldValueKey(p.cond)
		if !p.hasValueExpr {
			opt.Condition = p.cond
		} else {
			opt.Condition = influxql.Reduce(RewriteExprRemoveFieldValue(influxql.CloneExpr(p.cond)), nil)
			if IsTrueBooleanLiteral(opt.Condition) {
				opt.Condition = nil
			}
		}
	}

	p.sqry, err = viewer.CreateSeriesCursor(ctx, orgID, bucketID, opt.Condition)
	if err != nil {
		p.Close()
		return nil, tracing.LogError(span, err)
	}
	return p, nil
}

func (c *indexSeriesCursor) Close() {
	if !c.eof {
		c.eof = true
		if c.sqry != nil {
			c.sqry.Close()
			c.sqry = nil
		}
	}
}

func copyTags(dst, src models.Tags) models.Tags {
	if cap(dst) < src.Len() {
		dst = make(models.Tags, src.Len())
	} else {
		dst = dst[:src.Len()]
	}
	copy(dst, src)
	return dst
}

// Next emits a series row containing a series key and possible predicate on that series.
func (c *indexSeriesCursor) Next() *SeriesRow {
	if c.eof {
		return nil
	}

	// next series key
	sr, err := c.sqry.Next()
	if err != nil {
		c.err = err
		c.Close()
		return nil
	} else if sr == nil {
		c.Close()
		return nil
	}

	if len(sr.Tags) < 2 {
		// Invariant broken.
		c.err = fmt.Errorf("attempted to emit key with only tags: %s", sr.Tags)
		return nil
	}

	c.seriesRow.Name = sr.Name
	// TODO(edd): check this.
	c.seriesRow.SeriesTags = copyTags(c.seriesRow.SeriesTags, sr.Tags)
	c.seriesRow.Tags = copyTags(c.seriesRow.Tags, sr.Tags)

	if c.cond != nil && c.hasValueExpr {
		// TODO(sgc): lazily evaluate valueCond
		c.seriesRow.ValueCond = influxql.Reduce(c.cond, c)
		if IsTrueBooleanLiteral(c.seriesRow.ValueCond) {
			// we've reduced the expression to "true"
			c.seriesRow.ValueCond = nil
		}
	}

	// Normalise the special tag keys to the emitted format.
	mv := c.seriesRow.Tags.Get(models.MeasurementTagKeyBytes)
	c.seriesRow.Tags.Delete(models.MeasurementTagKeyBytes)
	c.seriesRow.Tags.Set(measurementKeyBytes, mv)

	fv := c.seriesRow.Tags.Get(models.FieldKeyTagKeyBytes)
	c.seriesRow.Field = string(fv)
	c.seriesRow.Tags.Delete(models.FieldKeyTagKeyBytes)
	c.seriesRow.Tags.Set(fieldKeyBytes, fv)

	return &c.seriesRow
}

func (c *indexSeriesCursor) Value(key string) (interface{}, bool) {
	res := c.seriesRow.Tags.Get([]byte(key))
	// Return res as a string so it compares correctly with the string literals
	return string(res), res != nil
}

func (c *indexSeriesCursor) Err() error {
	return c.err
}
