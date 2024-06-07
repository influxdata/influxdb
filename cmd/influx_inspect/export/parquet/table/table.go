package table

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/index"
	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/models"
	"github.com/influxdata/influxdb/cmd/influx_inspect/export/parquet/resultset"
	"go.uber.org/multierr"
	"golang.org/x/exp/maps"
)

// ReadStatistics tracks statistics after each call to Table.Next.
type ReadStatistics struct {
	// RowCount is the total number of rows read.
	RowCount int64
	// SeriesCount is the total number of series read.
	SeriesCount int64
}

type Table struct {
	mem *trackedAllocator
	rs  resultset.ResultSet
	err error
	// columns is the list of Columns defined by the schema.
	columns []Column
	// lastGroupKey
	lastGroupKey models.Escaped
	// timestamps is the builder for the timestamp columns
	timestamps timestampColumn
	// tags are the builders for the tag columns. The slice is sorted by tagColumn.name.
	tags []tagColumn
	// fields are the builders for the fields. The slice is sorted by FieldBuilder.Name.
	fields []FieldBuilder
	// cursor holds the cursor for the next group key
	cursor resultset.SeriesCursor
	// limit controls the amount of data read for each call to Next.
	limit ResourceLimit
	// buf contains various scratch buffers
	buf struct {
		tags         models.Tags
		arrays       []arrow.Array
		fieldCursors []fieldCursor
	}
	stats ReadStatistics
	// loadNextGroupKey is true if the last group key has been fully read.
	loadNextGroupKey bool
	// done is true when the result set has been fully read or an error has occurred.
	done bool
}

type options struct {
	limit       ResourceLimit
	concurrency int
}

// OptionFn defines the function signature for options to configure a new Table.
type OptionFn func(*options)

// WithResourceLimit sets the resource limit for the Table when calling Table.Next.
func WithResourceLimit(limit ResourceLimit) OptionFn {
	return func(o *options) {
		o.limit = limit
	}
}

// WithConcurrency sets the number of goroutines to use when reading the result set.
func WithConcurrency(concurrency int) OptionFn {
	return func(o *options) {
		o.concurrency = concurrency
	}
}

// New returns a new Table for the specified resultset.ResultSet.
func New(rs resultset.ResultSet, schema *index.MeasurementSchema, opts ...OptionFn) (*Table, error) {
	o := options{limit: ResourceLimitGroups(1)}
	for _, opt := range opts {
		opt(&o)
	}
	mem := &trackedAllocator{mem: memory.GoAllocator{}}

	tagNames := maps.Keys(schema.TagSet)
	slices.Sort(tagNames)

	var tags []tagColumn
	for _, name := range tagNames {
		tags = append(tags, tagColumn{name: name, b: array.NewStringBuilder(mem)})
	}

	fieldTypes := maps.Keys(schema.FieldSet)

	// check for fields conflicts
	seenFields := map[string]struct{}{}
	for i := range fieldTypes {
		f := &fieldTypes[i]
		if _, ok := seenFields[f.Name]; ok {
			return nil, fmt.Errorf("field conflict: field %s has multiple data types", f.Name)
		}
	}

	// fields are sorted in lexicographically ascending order
	slices.SortFunc(fieldTypes, func(a, b index.MeasurementField) int { return strings.Compare(a.Name, b.Name) })

	var fields []FieldBuilder
	for i := range fieldTypes {
		f := &fieldTypes[i]
		fields = append(fields, newField(mem, f.Name, f.Type))
	}

	columnCount := 1 + // timestamp column
		len(tags) + len(fields)

	res := &Table{
		mem:        mem,
		rs:         rs,
		columns:    make([]Column, 0, columnCount),
		timestamps: timestampColumn{b: array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Nanosecond})},
		tags:       tags,
		fields:     fields,
		limit:      o.limit,
		buf: struct {
			tags         models.Tags
			arrays       []arrow.Array
			fieldCursors []fieldCursor
		}{
			arrays:       make([]arrow.Array, columnCount),
			fieldCursors: make([]fieldCursor, len(fields)),
		},
		loadNextGroupKey: true,
	}

	res.columns = append(res.columns, &res.timestamps)
	for i := range tags {
		res.columns = append(res.columns, &tags[i])
	}
	for _, f := range fields {
		res.columns = append(res.columns, f)
	}

	return res, nil
}

// Columns returns the list of columns for the table.
// Inspecting the Column.SemanticType will indicate the semantic types.
//
// The first element is always the SemanticTypeTimestamp column.
// The next 0-n are SemanticTypeTag, and the remaining are SemanticTypeField.
func (t *Table) Columns() []Column { return t.columns }

// Err returns the last recorded error for the receiver.
func (t *Table) Err() error { return t.err }

func (t *Table) findField(name []byte) (field FieldBuilder) {
	slices.BinarySearchFunc(t.fields, name, func(c FieldBuilder, bytes []byte) int {
		if c.Name() < string(bytes) {
			return -1
		} else if c.Name() > string(bytes) {
			return 1
		} else {
			field = c
			return 0
		}
	})

	return
}

func (t *Table) loadNextKey(ctx context.Context) models.Escaped {
	var next resultset.SeriesCursor
	if t.cursor != nil {
		next = t.cursor
		t.cursor = nil
	} else {
		next = t.rs.Next(ctx)
	}

	if next == nil {
		t.rs.Close()
		t.err = t.rs.Err()
		t.done = true
		return models.Escaped{}
	}

	groupKey, fieldEsc, _ := models.SeriesAndField(next.SeriesKey())
	field := models.UnescapeToken(fieldEsc)
	fieldCursors := t.buf.fieldCursors[:0]
	if f := t.findField(field); f != nil {
		fieldCursors = append(fieldCursors, fieldCursor{
			field:  f,
			cursor: next,
		})
	}

	// find remaining fields for the current group key,
	for {
		cur := t.rs.Next(ctx)
		if cur == nil {
			break
		}

		nextGroup, fieldEsc, _ := models.SeriesAndField(cur.SeriesKey())
		if string(groupKey.B) != string(nextGroup.B) {
			// save this cursor, which is the next group key
			t.cursor = cur
			break
		}

		field := models.UnescapeToken(fieldEsc)
		if f := t.findField(field); f != nil {
			fieldCursors = append(fieldCursors, fieldCursor{
				field:  f,
				cursor: cur,
			})
		}
	}

	for i := 0; i < len(fieldCursors); i++ {
		// copy the fieldCursor
		fc := fieldCursors[i]
		// and ensure the fields of the struct are set to nil, so they can be garbage collected
		fieldCursors[i] = fieldCursor{}
		if err := fc.field.SetCursor(fc.cursor); err != nil {
			t.err = err
			return models.Escaped{}
		}
	}

	return groupKey
}

// Stats returns the read statistics for the table, accumulated after
// each call to Next.
// It is safe to call Stats concurrently.
func (t *Table) Stats() ReadStatistics {
	var stats ReadStatistics
	stats.RowCount = atomic.LoadInt64(&t.stats.RowCount)
	stats.SeriesCount = atomic.LoadInt64(&t.stats.SeriesCount)
	return stats
}

// ReadInfo contains additional information about the batch of data returned by Table.Next.
type ReadInfo struct {
	// FirstGroupKey is the first group key read by the batch.
	FirstGroupKey models.Escaped
	// LastGroupKey is the last group key read by the batch.
	LastGroupKey models.Escaped
}

// Next returns the next batch of data and information about the batch.
func (t *Table) Next(ctx context.Context) (columns []arrow.Array, readInfo ReadInfo) {
	if t.done {
		return
	}

	baseAllocated := t.mem.CurrentAlloc()

	var s resourceUsage
	defer func() {
		atomic.AddInt64(&t.stats.RowCount, int64(s.rowCount))
		readInfo.LastGroupKey = t.lastGroupKey
	}()

	for t.limit.isBelow(&s) {
		var groupKey models.Escaped
		if t.loadNextGroupKey {
			groupKey = t.loadNextKey(ctx)
			if len(groupKey.B) == 0 {
				// signals the end of the result set
				if s.rowCount > 0 {
					// return the remaining buffered data
					break
				}
				return
			}
			atomic.AddInt64(&t.stats.SeriesCount, 1)
			t.lastGroupKey = groupKey
			t.loadNextGroupKey = false
		} else {
			// continue reading current group key
			groupKey = t.lastGroupKey
		}

		if len(readInfo.FirstGroupKey.B) == 0 {
			readInfo.FirstGroupKey = groupKey
		}

		baseRowCount := s.rowCount
		// this loop reads the data for the current group key.
		for t.limit.isBelow(&s) {
			// find the next minimum timestamp from all the fields
			var minTs int64 = math.MaxInt64
			for _, col := range t.fields {
				if v, ok := col.PeekTimestamp(); ok && v < minTs {
					minTs = v
				}
			}

			if minTs == math.MaxInt64 {
				t.loadNextGroupKey = true
				break
			}

			t.timestamps.b.Append(arrow.Timestamp(minTs))

			for _, col := range t.fields {
				col.Append(minTs)
			}
			s.rowCount++
			s.allocated = uint(t.mem.CurrentAlloc() - baseAllocated)
		}

		s.groupCount++

		if t.loadNextGroupKey {
			// Reset the fields for the next group key
			for _, col := range t.fields {
				col.Reset()
				t.err = multierr.Append(t.err, col.Err())
			}
			if t.err != nil {
				t.done = true
				return
			}
		}

		groupRowCount := int(s.rowCount - baseRowCount)

		_, t.buf.tags = models.ParseKeyBytesWithTags(groupKey, t.buf.tags)
		tagSet := t.buf.tags[1:] // skip measurement, which is always the first tag key (\x00)

		// For each tag column, append the tag value for all rows of the current group; otherwise,
		// append nulls if the tag does not exist in this group.
		for i := range t.tags {
			tc := &t.tags[i]
			tc.b.Reserve(groupRowCount)

			// tagSet is sorted, so perform a binary search
			idx := sort.Search(len(tagSet), func(i int) bool {
				return string(tagSet[i].Key) >= tc.name
			})
			if idx < len(tagSet) && string(tagSet[idx].Key) == tc.name {
				value := tagSet[idx].Value
				for j := 0; j < groupRowCount; j++ {
					tc.b.BinaryBuilder.Append(value)
				}
			} else {
				tc.b.BinaryBuilder.AppendNulls(groupRowCount)
			}
		}
	}

	// build the final results
	columns = t.buf.arrays[:0]
	columns = append(columns, t.timestamps.b.NewArray())

	for i := range t.tags {
		columns = append(columns, t.tags[i].b.NewArray())
	}

	for _, b := range t.fields {
		columns = append(columns, b.NewArray())
	}

	return
}

type fieldCursor struct {
	field  FieldBuilder
	cursor resultset.SeriesCursor
}
