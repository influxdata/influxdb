package reads

import (
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
	SeriesTags models.Tags // unmodified series tags with measurement key as \x00, field key as \xff
	Tags       models.Tags // tags with measurement key as _measurement, field key as _field
	Field      string
	Query      cursors.CursorIterator
	ValueCond  influxql.Expr
}
