package table

import (
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/resultset"
	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/tsm1"
)

// SemanticType defines the possible semantic types of a column.
type SemanticType byte

const (
	// SemanticTypeTimestamp indicates the column is a timestamp.
	SemanticTypeTimestamp SemanticType = iota
	// SemanticTypeTag indicates the column is a tag.
	SemanticTypeTag
	// SemanticTypeField indicates the column is a field.
	SemanticTypeField
)

// Column provides name and data type information about a Table column.
type Column interface {
	// Name is the name of the receiver.
	Name() string

	// DataType is the data type of the receiver.
	DataType() arrow.DataType

	// SemanticType returns the semantic type of the receiver.
	SemanticType() SemanticType
}

// FieldBuilder defines the behavior for building an arrow.Array by
// consuming the data produced by a resultset.SeriesCursor.
type FieldBuilder interface {
	Column

	// SetCursor specifies the source of time series data for the receiver.
	// SetCursor returns an error if the resultset.TypedCursor does not match
	// the expected DataType or Reset has not been called after a previous call
	// to SetCursor.
	SetCursor(c resultset.SeriesCursor) error

	// NewArray returns an array of the current values for the receiver.
	NewArray() arrow.Array

	// PeekTimestamp returns the next timestamp of the receiver and a boolean
	// indicating if the timestamp is valid.
	PeekTimestamp() (int64, bool)

	// Append appends the value of the field, for the specified timestamp,
	// to the array or null if there is no value.
	Append(ts int64)

	// Err returns the error if the receiver failed to consume data from
	// the resultset.SeriesCursor.
	Err() error

	// Reset releases and resources from a previous call to SetCursor, and
	// prepares the FieldBuilder for a new resultset.SeriesCursor.
	Reset()
}

// arrayBuilder describes the common behavior required to build an arrow.Array.
type arrayBuilder[T resultset.BlockType] interface {
	AppendNull()
	Append(T)
	NewArray() arrow.Array
	Type() arrow.DataType
}

// typedField is a generic implementation of the FieldBuilder interface for
// all supported TSM block types.
type typedField[T resultset.BlockType] struct {
	name string
	cur  resultset.TypedCursor[T]
	bui  arrayBuilder[T]
	a    *resultset.TimeArray[T]
	i    int
	err  error
}

// newField constructs a new FieldBuilder with the specified column name and
// block type.
func newField(mem memory.Allocator, name string, bt tsm1.BlockType) FieldBuilder {
	var col FieldBuilder
	switch bt {
	case tsm1.BlockFloat64:
		col = &typedField[float64]{
			name: name,
			bui:  array.NewFloat64Builder(mem),
		}
	case tsm1.BlockInteger:
		col = &typedField[int64]{
			name: name,
			bui:  array.NewInt64Builder(mem),
		}
	case tsm1.BlockUnsigned:
		col = &typedField[uint64]{
			name: name,
			bui:  array.NewUint64Builder(mem),
		}
	case tsm1.BlockString:
		col = &typedField[string]{
			name: name,
			bui:  array.NewStringBuilder(mem),
		}
	case tsm1.BlockBoolean:
		col = &typedField[bool]{
			name: name,
			bui:  array.NewBooleanBuilder(mem),
		}
	default:
		panic("unreachable")
	}
	return col
}

func (c *typedField[T]) Name() string               { return c.name }
func (c *typedField[T]) DataType() arrow.DataType   { return c.bui.Type() }
func (c *typedField[T]) SemanticType() SemanticType { return SemanticTypeField }

func (c *typedField[T]) SetCursor(cur resultset.SeriesCursor) error {
	if c.err != nil {
		return c.err
	}

	tc, ok := cur.(resultset.TypedCursor[T])
	if !ok {
		return fmt.Errorf("field %s: unexpected cursor type: %T", c.name, cur)
	}
	if c.cur != nil {
		// Reset is expected
		return fmt.Errorf("field %s: SetCursor called with previous cursor", c.name)
	}
	c.cur = tc
	c.load()
	return nil
}

func (c *typedField[T]) NewArray() arrow.Array {
	return c.bui.NewArray()
}

func (c *typedField[T]) load() {
	c.a = c.cur.Next()
	if c.a == nil {
		c.Reset()
	} else {
		c.i = 0
	}
}

func (c *typedField[T]) PeekTimestamp() (int64, bool) {
	if c.i >= c.a.Len() {
		return 0, false
	}

	return c.a.Timestamps[c.i], true
}

func (c *typedField[T]) Append(ts int64) {
	if c.i < c.a.Len() && c.a.Timestamps[c.i] == ts {
		v := c.a.Values[c.i]
		c.bui.Append(v)
		c.i++
		if c.i >= len(c.a.Timestamps) {
			c.load()
		}
	} else {
		c.bui.AppendNull()
	}
}

func (c *typedField[T]) Err() error { return c.err }

func (c *typedField[T]) Reset() {
	if c.cur != nil {
		c.cur.Close()
		c.err = c.cur.Err()
		c.cur = nil
	}
}

// tagColumn contains state for building an arrow.Array for a tag column.
type tagColumn struct {
	name string
	b    *array.StringBuilder
}

func (t *tagColumn) Name() string               { return t.name }
func (t *tagColumn) DataType() arrow.DataType   { return t.b.Type() }
func (t *tagColumn) SemanticType() SemanticType { return SemanticTypeTag }

// timestampColumn contains state for building an arrow.Array for a timestamp column.
type timestampColumn struct {
	b *array.TimestampBuilder
}

func (t *timestampColumn) Name() string               { return "time" }
func (t *timestampColumn) DataType() arrow.DataType   { return t.b.Type() }
func (t *timestampColumn) SemanticType() SemanticType { return SemanticTypeTimestamp }
