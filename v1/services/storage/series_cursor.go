package storage

import (
	"context"
	"sort"

	"github.com/influxdata/influxdb/v2/influxql/query"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/slices"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxql"
	opentracing "github.com/opentracing/opentracing-go"
)

const (
	measurementKey = "_measurement"
	fieldKey       = "_field"
)

var (
	measurementKeyBytes = []byte(measurementKey)
	fieldKeyBytes       = []byte(fieldKey)
)

type indexSeriesCursor struct {
	sqry            tsdb.SeriesCursor
	fields          measurementFields
	nf              []field
	field           field
	err             error
	tags            models.Tags
	cond            influxql.Expr
	measurementCond influxql.Expr
	row             reads.SeriesRow
	eof             bool
	hasFieldExpr    bool
	hasValueExpr    bool
}

func newIndexSeriesCursor(ctx context.Context, predicate *datatypes.Predicate, shards []*tsdb.Shard) (*indexSeriesCursor, error) {
	var expr influxql.Expr
	if root := predicate.GetRoot(); root != nil {
		var err error
		if expr, err = reads.NodeToExpr(root, measurementRemap); err != nil {
			return nil, err
		}
	}

	return newIndexSeriesCursorInfluxQLPred(ctx, expr, shards)
}

func newIndexSeriesCursorInfluxQLPred(ctx context.Context, predicate influxql.Expr, shards []*tsdb.Shard) (*indexSeriesCursor, error) {
	queries, err := tsdb.CreateCursorIterators(ctx, shards)
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
	p := &indexSeriesCursor{row: reads.SeriesRow{Query: queries}}

	if predicate != nil {
		p.cond = predicate

		p.hasFieldExpr, p.hasValueExpr = HasFieldKeyOrValue(p.cond)
		if !(p.hasFieldExpr || p.hasValueExpr) {
			p.measurementCond = p.cond
			opt.Condition = p.cond
		} else {
			p.measurementCond = influxql.Reduce(reads.RewriteExprRemoveFieldValue(influxql.CloneExpr(p.cond)), nil)
			if reads.IsTrueBooleanLiteral(p.measurementCond) {
				p.measurementCond = nil
			}

			opt.Condition = influxql.Reduce(RewriteExprRemoveFieldKeyAndValue(influxql.CloneExpr(p.cond)), nil)
			if reads.IsTrueBooleanLiteral(opt.Condition) {
				opt.Condition = nil
			}
		}
	}

	sg := tsdb.Shards(shards)
	if mfkeys, err := sg.FieldKeysByPredicate(opt.Condition); err == nil {
		p.fields = make(map[string][]field, len(mfkeys))
		fieldNames := []string{}
		for name, fkeys := range mfkeys {
			fields := make([]field, 0, len(fkeys))
			for _, key := range fkeys {
				fields = append(fields, field{n: key, nb: []byte(key)})
			}
			p.fields[name] = fields
			fieldNames = append(fieldNames, name)
		}

		sort.Strings(fieldNames)
		mitr := tsdb.NewMeasurementSliceIterator(slices.StringsToBytes(fieldNames...))
		p.sqry, err = sg.CreateSeriesCursor(ctx, tsdb.SeriesCursorRequest{Measurements: mitr}, opt.Condition)
		if p.sqry != nil && err == nil {
			return p, nil
		}
	}

	p.Close()
	return nil, err
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

func (c *indexSeriesCursor) Next() *reads.SeriesRow {
	if c.eof {
		return nil
	}

	for {
		if len(c.nf) == 0 {
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
			c.row.SeriesTags = sr.Tags
			c.tags = copyTags(c.tags, sr.Tags)
			c.tags.Set(measurementKeyBytes, sr.Name)

			c.nf = c.fields[string(sr.Name)]
			// c.nf may be nil if there are no fields
		} else {
			c.field, c.nf = c.nf[0], c.nf[1:]

			if c.measurementCond == nil || reads.EvalExprBool(c.measurementCond, c) {
				break
			}
		}
	}

	c.tags.Set(fieldKeyBytes, c.field.nb)
	c.row.Field = c.field.n

	if c.cond != nil && c.hasValueExpr {
		// TODO(sgc): lazily evaluate valueCond
		c.row.ValueCond = influxql.Reduce(c.cond, c)
		if reads.IsTrueBooleanLiteral(c.row.ValueCond) {
			// we've reduced the expression to "true"
			c.row.ValueCond = nil
		}
	}

	c.row.Tags = copyTags(c.row.Tags, c.tags)

	return &c.row
}

func (c *indexSeriesCursor) Value(key string) (interface{}, bool) {
	switch key {
	case "_name":
		return string(c.row.Name), true
	case fieldKey:
		return c.field.n, true
	case "$":
		return nil, false
	default:
		res := c.row.SeriesTags.GetString(key)
		return res, true
	}
}

func (c *indexSeriesCursor) Err() error {
	return c.err
}

type measurementFields map[string][]field

type field struct {
	n  string
	nb []byte
}
