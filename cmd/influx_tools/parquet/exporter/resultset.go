package export

import (
	"bytes"
	"context"
	"fmt"

	models2 "github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/models"
	"github.com/influxdata/influxdb/cmd/influx_tools/parquet/exporter/parquet/resultset"
	"github.com/influxdata/influxdb/models"
	"golang.org/x/exp/maps"
)

func NewResultSet(measurement string, data map[string]map[string]ShardValues) resultset.ResultSet {
	return &resultSet{
		measurement: []byte(measurement),
		data:        data,
		keys:        maps.Keys(data),
	}
}

// resultSet implements resultset.ResultSet over v1 shard data
type resultSet struct {
	measurement []byte
	data        map[string]map[string]ShardValues
	keys        []string
	keyIndex    int
	fields      []string
	fieldIndex  int
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
	seriesKey := makeV2SeriesKey(r.measurement, []byte(groupKey), []byte(fieldName))

	switch values.Type().(type) {
	case int64:
		return newExportedCursor[int64](seriesKey, values)
	case float64:
		return newExportedCursor[float64](seriesKey, values)
	case string:
		return newExportedCursor[string](seriesKey, values)
	case uint64:
		return newExportedCursor[uint64](seriesKey, values)
	case bool:
		return newExportedCursor[bool](seriesKey, values)
	}

	return nil
}

func (r *resultSet) Close() {
}

func (r *resultSet) Err() error {
	return nil
}

func makeV2SeriesKey(measurement, groupKey, fieldName []byte) models2.Escaped {
	var b bytes.Buffer
	b.WriteString("NULL,")
	b.Write(models.MeasurementTagKeyBytes)
	b.WriteByte('=')
	b.Write(measurement)
	b.WriteByte(',')
	b.Write(groupKey)
	b.WriteByte(',')
	b.Write(models.FieldKeyTagKeyBytes)
	b.WriteByte('=')
	b.Write(fieldName)
	//b.WriteString("#!~#")
	//b.Write(fieldName)
	return models2.MakeEscaped(b.Bytes())
}

// exportedCursor implements resultset.TypedCursor for v1 shard data
type exportedCursor[T resultset.BlockType] struct {
	seriesKey  models2.Escaped
	chunks     []*resultset.TimeArray[T]
	chunkIndex int
}

func newExportedCursor[T resultset.BlockType](seriesKey models2.Escaped, values ShardValues) *exportedCursor[T] {
	c := &exportedCursor[T]{
		seriesKey: seriesKey,
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

	fmt.Println("new series cursor")
	fmt.Printf("  series key: %s\n", c.seriesKey.S())
	fmt.Printf("  data: %T lens: %d:%d\n", values.Type(), len(c.chunks[0].Timestamps), len(c.chunks[0].Values))

	return c
}

func (c *exportedCursor[T]) SeriesKey() models2.Escaped {
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
