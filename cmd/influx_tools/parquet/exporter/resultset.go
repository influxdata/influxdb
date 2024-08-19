package export

import (
	"context"
	"fmt"
	"github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/models"
	"github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/resultset"
	"golang.org/x/exp/maps"
)

func NewResultSet(data map[models.EscapedString]map[models.EscapedString]ShardValues) resultset.ResultSet {
	return &resultSet{
		data: data,
		keys: maps.Keys(data),
	}
}

// resultSet implements resultset.ResultSet over shard data
type resultSet struct {
	data       map[models.EscapedString]map[models.EscapedString]ShardValues
	keys       []models.EscapedString
	keyIndex   int
	fields     []models.EscapedString
	fieldIndex int
}

func (r *resultSet) Next(ctx context.Context) resultset.SeriesCursor {
	if r.keyIndex >= len(r.keys) {
		return nil
	}

	if r.fields == nil {
		r.fields = maps.Keys(r.data[r.keys[r.keyIndex]])
	}

	if r.fieldIndex >= len(r.fields) {
		r.fields = nil
		r.fieldIndex = 0
		r.keyIndex++
		return r.Next(ctx)
	}

	defer func() {
		r.fieldIndex++
	}()

	groupKey := r.keys[r.keyIndex]
	fieldName := r.fields[r.fieldIndex]
	values := r.data[groupKey][fieldName]

	switch values.Type().(type) {
	case int64:
		return newExportedCursor[int64](groupKey, fieldName, values)
	case float64:
		return newExportedCursor[float64](groupKey, fieldName, values)
	case string:
		return newExportedCursor[string](groupKey, fieldName, values)
	case uint64:
		return newExportedCursor[uint64](groupKey, fieldName, values)
	case bool:
		return newExportedCursor[bool](groupKey, fieldName, values)
	}

	return nil
}

func (r *resultSet) Close() {
}

func (r *resultSet) Err() error {
	return nil
}

// exportedCursor implements resultset.TypedCursor for shard data
type exportedCursor[T resultset.BlockType] struct {
	seriesKey  models.Escaped
	chunks     []*resultset.TimeArray[T]
	chunkIndex int
}

func newExportedCursor[T resultset.BlockType](groupKey, fieldName models.EscapedString, values ShardValues) *exportedCursor[T] {
	seriesKey := append(groupKey.B().B, []byte(","+models.FieldKeyTagKey+"=")...)
	seriesKey = append(seriesKey, fieldName.B().B...)

	c := &exportedCursor[T]{
		seriesKey: models.MakeEscaped(seriesKey),
		chunks: []*resultset.TimeArray[T]{ // a single chunk, for now...
			&resultset.TimeArray[T]{},
		},
	}

	// TODO is this zero-copy safe?
	c.chunks[0].Timestamps = values.Timestamps()
	c.chunks[0].Values = values.Values().([]T)

	// TODO paranoia check
	if len(c.chunks[0].Timestamps) != len(c.chunks[0].Values) {
		panic("len(timestamps) != len(values)")
	}

	fmt.Println("new v2 series cursor")
	fmt.Printf("  key: %s\n", c.seriesKey.S())
	fmt.Printf("  data: %T lens: %d:%d\n", values.Type(), len(c.chunks[0].Timestamps), len(c.chunks[0].Values))

	return c
}

func (c *exportedCursor[T]) SeriesKey() models.Escaped {
	return c.seriesKey
}

func (c *exportedCursor[T]) Close() {
}

func (c *exportedCursor[T]) Err() error {
	return nil
}

func (c *exportedCursor[T]) Next() *resultset.TimeArray[T] {
	if c.chunkIndex >= len(c.chunks) {
		return nil
	}
	defer func() {
		c.chunkIndex++
	}()
	return c.chunks[c.chunkIndex]
}

// ShardValues is a helper bridging data from shard
type ShardValues interface {
	Type() interface{}
	Timestamps() []int64
	Values() interface{}
	Len() int
	String() string
}
