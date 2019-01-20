package storage

import (
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
)

// fixedSeriesIterator is a tsdb.SeriesIterator over a given set of keys.
type fixedSeriesIterator struct{ keys [][]byte }

// newFixedSeriesIterator returns a tsdb.SeriesIterator over the given set of keys.
func newFixedSeriesIterator(keys [][]byte) *fixedSeriesIterator {
	return &fixedSeriesIterator{
		keys: keys,
	}
}

// Close is required for the tsdb.SeriesIterator interface.
func (i *fixedSeriesIterator) Close() error { return nil }

// Next advances the iterator and returns the current element.
func (i *fixedSeriesIterator) Next() (tsdb.SeriesElem, error) {
	if len(i.keys) == 0 {
		return nil, nil
	}
	key := i.keys[0]
	i.keys = i.keys[1:]

	name, tags := models.ParseKeyBytes(key)
	return &fixedSeriesIteratorElem{
		name: name,
		tags: tags,
	}, nil
}

// fixedSeriesIteratorElem imlements tsdb.SeriesElem but only the name and tags.
type fixedSeriesIteratorElem struct {
	name []byte
	tags models.Tags
}

func (i *fixedSeriesIteratorElem) Name() []byte        { return i.name }
func (i *fixedSeriesIteratorElem) Tags() models.Tags   { return i.tags }
func (i *fixedSeriesIteratorElem) Deleted() bool       { return false }
func (i *fixedSeriesIteratorElem) Expr() influxql.Expr { return nil }
