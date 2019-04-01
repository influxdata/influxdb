package reads

import (
	"context"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/cursors"
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
	Tags       models.Tags
	Field      string
	Query      cursors.CursorIterators
	ValueCond  influxql.Expr
}

type limitSeriesCursor struct {
	SeriesCursor
	n, o, c int64
}

func NewLimitSeriesCursor(ctx context.Context, cur SeriesCursor, n, o int64) SeriesCursor {
	return &limitSeriesCursor{SeriesCursor: cur, o: o, n: n}
}

func (c *limitSeriesCursor) Next() *SeriesRow {
	if c.o > 0 {
		for i := int64(0); i < c.o; i++ {
			if c.SeriesCursor.Next() == nil {
				break
			}
		}
		c.o = 0
	}

	if c.c >= c.n {
		return nil
	}
	c.c++
	return c.SeriesCursor.Next()
}
