package storage

import (
	"context"
	"errors"
	"sort"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

type seriesCursor interface {
	Close()
	Next() *seriesRow
	Err() error
}

type seriesRow struct {
	name  []byte      // measurement name
	tags  models.Tags // unmodified series tags
	field field
	query tsdb.CursorIterators
}

type indexSeriesCursor struct {
	sqry   tsdb.SeriesCursor
	fields measurementFields
	nf     []field
	err    error
	row    seriesRow
	eof    bool
}

func newIndexSeriesCursor(ctx context.Context, shards []*tsdb.Shard) (*indexSeriesCursor, error) {
	queries, err := tsdb.CreateCursorIterators(ctx, shards)
	if err != nil {
		return nil, err
	}

	if queries == nil {
		return nil, nil
	}

	p := &indexSeriesCursor{row: seriesRow{query: queries}}

	sg := tsdb.Shards(shards)
	p.sqry, err = sg.CreateSeriesCursor(ctx, tsdb.SeriesCursorRequest{}, nil)
	if p.sqry != nil && err == nil {
		var itr query.Iterator
		var fi query.FloatIterator
		var opt = query.IteratorOptions{
			Aux:        []influxql.VarRef{{Val: "key"}},
			Authorizer: query.OpenAuthorizer,
			Ascending:  true,
			Ordered:    true,
		}

		if itr, err = sg.CreateIterator(ctx, &influxql.Measurement{SystemIterator: "_fieldKeys"}, opt); itr != nil && err == nil {
			if fi, err = toFloatIterator(itr); err != nil {
				goto CLEANUP
			}

			p.fields = extractFields(fi)
			fi.Close()
			return p, nil
		}
	}

CLEANUP:
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

func (c *indexSeriesCursor) Next() *seriesRow {
	if c.eof {
		return nil
	}

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

		c.row.name = sr.Name
		c.row.tags = sr.Tags

		c.nf = c.fields[string(sr.Name)]
	}

	c.row.field, c.nf = c.nf[0], c.nf[1:]

	return &c.row
}

func (c *indexSeriesCursor) Err() error {
	return c.err
}

func toFloatIterator(iter query.Iterator) (query.FloatIterator, error) {
	sitr, ok := iter.(query.FloatIterator)
	if !ok {
		return nil, errors.New("expected FloatIterator")
	}

	return sitr, nil
}

type measurementFields map[string][]field

type field struct {
	n string
	d influxql.DataType
}

func extractFields(itr query.FloatIterator) measurementFields {
	mf := make(measurementFields)

	for {
		p, err := itr.Next()
		if err != nil {
			return nil
		} else if p == nil {
			break
		}

		// Aux is populated by `fieldKeysIterator#Next`
		fields := append(mf[p.Name], field{
			n: p.Aux[0].(string),
			d: influxql.DataTypeFromString(p.Aux[1].(string)),
		})

		mf[p.Name] = fields
	}

	if len(mf) == 0 {
		return nil
	}

	for k, fields := range mf {
		sort.Slice(fields, func(i, j int) bool {
			return fields[i].n < fields[j].n
		})

		// deduplicate
		i := 1
		for j := 1; j < len(fields); j++ {
			if fields[j].n != fields[j-1].n {
				fields[i] = fields[j]
				i++
			}
		}

		mf[k] = fields[:i]
	}

	return mf
}
