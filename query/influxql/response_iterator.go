package influxql

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/arrow"
	"github.com/influxdata/flux/execute"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/values"
)

// responseIterator implements flux.ResultIterator for a Response.
type responseIterator struct {
	response  *Response
	resultIdx int
}

// NewResponseIterator constructs a flux.ResultIterator from a Response.
func NewResponseIterator(r *Response) flux.ResultIterator {
	return &responseIterator{
		response: r,
	}
}

// More returns true if there are results left to iterate through.
// It is used to implement flux.ResultIterator.
func (r *responseIterator) More() bool {
	return r.resultIdx < len(r.response.Results)
}

// Next retrieves the next flux.Result.
// It is used to implement flux.ResultIterator.
func (r *responseIterator) Next() flux.Result {
	res := r.response.Results[r.resultIdx]
	r.resultIdx++
	return newQueryResult(&res)
}

// Release is a noop.
// It is used to implement flux.ResultIterator.
func (r *responseIterator) Release() {}

// Err returns an error if the response contained an error.
// It is used to implement flux.ResultIterator.
func (r *responseIterator) Err() error {
	if r.response.Err != "" {
		return fmt.Errorf(r.response.Err)
	}

	return nil
}

func (r *responseIterator) Statistics() flux.Statistics {
	return flux.Statistics{}
}

// seriesIterator is a simple wrapper for Result that implements flux.Result and flux.TableIterator.
type seriesIterator struct {
	result *Result
}

func newQueryResult(r *Result) *seriesIterator {
	return &seriesIterator{
		result: r,
	}
}

// Name returns the results statement id.
// It is used to implement flux.Result.
func (r *seriesIterator) Name() string {
	return strconv.Itoa(r.result.StatementID)
}

// Tables returns the original as a flux.TableIterator.
// It is used to implement flux.Result.
func (r *seriesIterator) Tables() flux.TableIterator {
	return r
}

// Do iterates through the series of a Result.
// It is used to implement flux.TableIterator.
func (r *seriesIterator) Do(f func(flux.Table) error) error {
	for _, row := range r.result.Series {
		t, err := newQueryTable(row)
		if err != nil {
			return err
		}
		if err := f(t); err != nil {
			return err
		}
	}

	return nil
}

func (r *seriesIterator) Statistics() flux.Statistics {
	return flux.Statistics{}
}

// queryTable implements flux.Table and flux.ColReader.
type queryTable struct {
	row      *Row
	groupKey flux.GroupKey
	colMeta  []flux.ColMeta
	cols     []array.Interface
}

func newQueryTable(r *Row) (*queryTable, error) {
	t := &queryTable{
		row: r,
	}
	if err := t.translateRowsToColumns(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *queryTable) Statistics() flux.Statistics {
	return flux.Statistics{}
}

// Data in a column is laid out in the following way:
//   [ r.row.Columns... , r.tagKeys()... , r.row.Name ]
func (t *queryTable) translateRowsToColumns() error {
	t.cols = make([]array.Interface, len(t.Cols()))
	for i := range t.row.Columns {
		col := t.Cols()[i]
		switch col.Type {
		case flux.TFloat:
			b := arrow.NewFloatBuilder(&memory.Allocator{})
			b.Reserve(t.Len())
			for _, row := range t.row.Values {
				val, ok := row[i].(float64)
				if !ok {
					return fmt.Errorf("unsupported type %T found in column %s of type %s", val, col.Label, col.Type)
				}
				b.Append(val)
			}
			t.cols[i] = b.NewArray()
			b.Release()
		case flux.TInt:
			b := arrow.NewIntBuilder(&memory.Allocator{})
			b.Reserve(t.Len())
			for _, row := range t.row.Values {
				val, ok := row[i].(int64)
				if !ok {
					return fmt.Errorf("unsupported type %T found in column %s of type %s", val, col.Label, col.Type)
				}
				b.Append(val)
			}
			t.cols[i] = b.NewArray()
			b.Release()
		case flux.TUInt:
			b := arrow.NewUintBuilder(&memory.Allocator{})
			b.Reserve(t.Len())
			for _, row := range t.row.Values {
				val, ok := row[i].(uint64)
				if !ok {
					return fmt.Errorf("unsupported type %T found in column %s of type %s", val, col.Label, col.Type)
				}
				b.Append(val)
			}
			t.cols[i] = b.NewArray()
			b.Release()
		case flux.TString:
			b := arrow.NewStringBuilder(&memory.Allocator{})
			b.Reserve(t.Len())
			for _, row := range t.row.Values {
				val, ok := row[i].(string)
				if !ok {
					return fmt.Errorf("unsupported type %T found in column %s of type %s", val, col.Label, col.Type)
				}
				b.AppendString(val)
			}
			t.cols[i] = b.NewArray()
			b.Release()
		case flux.TBool:
			b := arrow.NewBoolBuilder(&memory.Allocator{})
			b.Reserve(t.Len())
			for _, row := range t.row.Values {
				val, ok := row[i].(bool)
				if !ok {
					return fmt.Errorf("unsupported type %T found in column %s of type %s", val, col.Label, col.Type)
				}
				b.Append(val)
			}
			t.cols[i] = b.NewArray()
			b.Release()
		case flux.TTime:
			b := arrow.NewIntBuilder(&memory.Allocator{})
			b.Reserve(t.Len())
			for _, row := range t.row.Values {
				switch val := row[i].(type) {
				case int64:
					b.Append(val)
				case float64:
					b.Append(int64(val))
				case string:
					tm, err := time.Parse(time.RFC3339, val)
					if err != nil {
						return fmt.Errorf("could not parse string %q as time: %v", val, err)
					}
					b.Append(tm.UnixNano())
				default:
					return fmt.Errorf("unsupported type %T found in column %s", val, col.Label)
				}
			}
			t.cols[i] = b.NewArray()
			b.Release()
		default:
			return fmt.Errorf("invalid type %T found in column %s", col.Type, col.Label)
		}
	}

	for j := len(t.row.Columns); j < len(t.Cols()); j++ {
		b := arrow.NewStringBuilder(&memory.Allocator{})
		b.Reserve(t.Len())

		var value string
		if key := t.Cols()[j].Label; key == "_measurement" {
			value = t.row.Name
		} else {
			value = t.row.Tags[key]
		}

		for i := 0; i < t.Len(); i++ {
			b.AppendString(value)
		}
		t.cols[j] = b.NewArray()
		b.Release()
	}
	return nil
}

// Key constructs the flux.GroupKey for a Row from the rows
// tags and measurement.
// It is used to implement flux.Table and flux.ColReader.
func (r *queryTable) Key() flux.GroupKey {
	if r.groupKey == nil {
		cols := make([]flux.ColMeta, len(r.row.Tags)+1) // plus one is for measurement
		vs := make([]values.Value, len(r.row.Tags)+1)
		kvs := make([]interface{}, len(r.row.Tags)+1)
		colMeta := r.Cols()
		labels := append(r.tagKeys(), "_measurement")
		for j, label := range labels {
			idx := execute.ColIdx(label, colMeta)
			if idx < 0 {
				panic(fmt.Errorf("table invalid: missing group column %q", label))
			}
			cols[j] = colMeta[idx]
			kvs[j] = "string"
			v := values.New(kvs[j])
			if v == values.InvalidValue {
				panic(fmt.Sprintf("unsupported value kind %T", kvs[j]))
			}
			vs[j] = v
		}
		r.groupKey = execute.NewGroupKey(cols, vs)
	}

	return r.groupKey
}

// tags returns the tag keys for a Row.
func (r *queryTable) tagKeys() []string {
	tags := []string{}
	for t := range r.row.Tags {
		tags = append(tags, t)
	}
	sort.Strings(tags)
	return tags
}

// Cols returns the columns for a row where the data is laid out in the following way:
//   [ r.row.Columns... , r.tagKeys()... , r.row.Name ]
// It is used to implement flux.Table and flux.ColReader.
func (r *queryTable) Cols() []flux.ColMeta {
	if r.colMeta == nil {
		colMeta := make([]flux.ColMeta, len(r.row.Columns)+len(r.row.Tags)+1)
		for i, col := range r.row.Columns {
			colMeta[i] = flux.ColMeta{
				Label: col,
				Type:  flux.TInvalid,
			}
			if col == "time" {
				// rename the time column
				colMeta[i].Label = "_time"
				colMeta[i].Type = flux.TTime
			}
		}

		if len(r.row.Values) < 1 {
			panic("must have at least one value")
		}
		data := r.row.Values[0]
		for i := range r.row.Columns {
			v := data[i]
			if colMeta[i].Label == "_time" {
				continue
			}
			switch v.(type) {
			case float64:
				colMeta[i].Type = flux.TFloat
			case int64:
				colMeta[i].Type = flux.TInt
			case uint64:
				colMeta[i].Type = flux.TUInt
			case bool:
				colMeta[i].Type = flux.TBool
			case string:
				colMeta[i].Type = flux.TString
			}
		}

		tags := r.tagKeys()

		leng := len(r.row.Columns)
		for i, tag := range tags {
			colMeta[leng+i] = flux.ColMeta{
				Label: tag,
				Type:  flux.TString,
			}
		}

		leng = leng + len(tags)
		colMeta[leng] = flux.ColMeta{
			Label: "_measurement",
			Type:  flux.TString,
		}
		r.colMeta = colMeta
	}

	return r.colMeta
}

// Do applies f to itself. This is because Row is a flux.ColReader.
// It is used to implement flux.Table.
func (r *queryTable) Do(f func(flux.ColReader) error) error {
	return f(r)
}

// RefCount is a noop.
// It is used to implement flux.ColReader.
func (r *queryTable) RefCount(n int) {}

// Empty returns true if a Row has no values.
// It is used to implement flux.Table.
func (r *queryTable) Empty() bool { return r.Len() == 0 }

// Len returns the length or r.row.Values
// It is used to implement flux.ColReader.
func (r *queryTable) Len() int {
	return len(r.row.Values)
}

// Bools returns the values in column index j as bools.
// It will panic if the column is not a []bool.
// It is used to implement flux.ColReader.
func (r *queryTable) Bools(j int) *array.Boolean {
	return r.cols[j].(*array.Boolean)
}

// Ints returns the values in column index j as ints.
// It will panic if the column is not a []int64.
// It is used to implement flux.ColReader.
func (r *queryTable) Ints(j int) *array.Int64 {
	return r.cols[j].(*array.Int64)
}

// UInts returns the values in column index j as ints.
// It will panic if the column is not a []uint64.
// It is used to implement flux.ColReader.
func (r *queryTable) UInts(j int) *array.Uint64 {
	return r.cols[j].(*array.Uint64)
}

// Floats returns the values in column index j as floats.
// It will panic if the column is not a []float64.
// It is used to implement flux.ColReader.
func (r *queryTable) Floats(j int) *array.Float64 {
	return r.cols[j].(*array.Float64)
}

// Strings returns the values in column index j as strings.
// It will panic if the column is not a []string.
// It is used to implement flux.ColReader.
func (r *queryTable) Strings(j int) *array.Binary {
	return r.cols[j].(*array.Binary)
}

// Times returns the values in column index j as values.Times.
// It will panic if the column is not a []values.Time.
// It is used to implement flux.ColReader.
func (r *queryTable) Times(j int) *array.Int64 {
	return r.cols[j].(*array.Int64)
}
