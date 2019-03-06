package readservice

import (
	"bytes"
	"context"
	"fmt"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	opentracing "github.com/opentracing/opentracing-go"
)

const (
	fieldKey       = "_field"
	measurementKey = "_measurement"
)

var (
	fieldKeyBytes       = []byte(fieldKey)
	measurementKeyBytes = []byte(measurementKey)
)

type indexSeriesCursor struct {
	sqry         storage.SeriesCursor
	err          error
	cond         influxql.Expr
	row          reads.SeriesRow
	eof          bool
	hasValueExpr bool
}

func newIndexSeriesCursor(ctx context.Context, src *readSource, req *datatypes.ReadRequest, engine *storage.Engine) (*indexSeriesCursor, error) {
	queries, err := engine.CreateCursorIterator(ctx)
	if err != nil {
		return nil, err
	}

	if queries == nil {
		return nil, nil
	}

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span = opentracing.StartSpan("index_cursor.create", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	opt := query.IteratorOptions{
		Aux:        []influxql.VarRef{{Val: "key"}},
		Authorizer: query.OpenAuthorizer,
		Ascending:  true,
		Ordered:    true,
	}
	p := &indexSeriesCursor{row: reads.SeriesRow{Query: tsdb.CursorIterators{queries}}}

	if root := req.Predicate.GetRoot(); root != nil {
		if p.cond, err = reads.NodeToExpr(root, nil); err != nil {
			return nil, err
		}

		p.hasValueExpr = reads.HasFieldValueKey(p.cond)
		if !p.hasValueExpr {
			opt.Condition = p.cond
		} else {
			opt.Condition = influxql.Reduce(reads.RewriteExprRemoveFieldValue(influxql.CloneExpr(p.cond)), nil)
			if reads.IsTrueBooleanLiteral(opt.Condition) {
				opt.Condition = nil
			}
		}
	}

	scr := storage.SeriesCursorRequest{
		Name: tsdb.EncodeName(platform.ID(src.OrganizationID), platform.ID(src.BucketID)),
	}
	p.sqry, err = engine.CreateSeriesCursor(ctx, scr, opt.Condition)
	if err != nil {
		p.Close()
		return nil, err
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
func (c *indexSeriesCursor) Next() *reads.SeriesRow {
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

	c.row.Name = sr.Name
	//TODO(edd): check this.
	c.row.SeriesTags = copyTags(c.row.SeriesTags, sr.Tags)
	c.row.Tags = copyTags(c.row.Tags, sr.Tags)
	c.row.Field = string(c.row.Tags.Get(models.FieldKeyTagKeyBytes))

	// Normalise the special tag keys to the emitted format.
	if len(c.row.Tags) < 2 {
		// Invariant broken.
		c.err = fmt.Errorf("attempted to emit key with only tags: %s", c.row.Tags)
		return &c.row
	}

	for i := 0; i < len(c.row.Tags); i++ {
		if bytes.Equal(c.row.Tags[i].Key, models.MeasurementTagKeyBytes) {
			c.row.Tags[i].Key = measurementKeyBytes
		} else if bytes.Equal(c.row.Tags[i].Key, models.FieldKeyTagKeyBytes) {
			c.row.Tags[i].Key = fieldKeyBytes
		}
	}

	if c.cond != nil && c.hasValueExpr {
		// TODO(sgc): lazily evaluate valueCond
		c.row.ValueCond = influxql.Reduce(c.cond, c)
		if reads.IsTrueBooleanLiteral(c.row.ValueCond) {
			// we've reduced the expression to "true"
			c.row.ValueCond = nil
		}
	}

	return &c.row
}

func (c *indexSeriesCursor) Value(key string) (interface{}, bool) {
	res := c.row.Tags.Get([]byte(key))
	// Return res as a string so it compares correctly with the string literals
	return string(res), res != nil
}

func (c *indexSeriesCursor) Err() error {
	return c.err
}
