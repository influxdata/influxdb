package influxql

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/values"
)

// responseIterator implements query.ResultIterator for a Response.
type responseIterator struct {
	response  *Response
	resultIdx int
}

// NewresponseIterator constructs a responseIterator from a query.ResultIterator.
func NewResponseIterator(r *Response) query.ResultIterator {
	return &responseIterator{
		response: r,
	}
}

// More returns true if there are results left to iterate through.
// It is used to implement query.ResultIterator.
func (r *responseIterator) More() bool {
	return r.resultIdx < len(r.response.Results)
}

// Next retrieves the next query.Result.
// It is used to implement query.ResultIterator.
func (r *responseIterator) Next() query.Result {
	res := r.response.Results[r.resultIdx]
	r.resultIdx++
	return newQueryResult(&res)
}

// Cancel is a noop.
// It is used to implement query.ResultIterator.
func (r *responseIterator) Cancel() {}

// Err returns an error if the response contained an error.
// It is used to implement query.ResultIterator.
func (r *responseIterator) Err() error {
	if r.response.Err != "" {
		return fmt.Errorf(r.response.Err)
	}

	return nil
}

// seriesIterator is a simple wrapper for Result that implements query.Result and query.TableIterator.
type seriesIterator struct {
	result *Result
}

func newQueryResult(r *Result) *seriesIterator {
	return &seriesIterator{
		result: r,
	}
}

// Name returns the results statement id.
// It is used to implement query.Result.
func (r *seriesIterator) Name() string {
	return strconv.Itoa(r.result.StatementID)
}

// Tables returns the original as a query.TableIterator.
// It is used to implement query.Result.
func (r *seriesIterator) Tables() query.TableIterator {
	return r
}

// Do iterates through the series of a Result.
// It is used to implement query.TableIterator.
func (r *seriesIterator) Do(f func(query.Table) error) error {
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

// queryTable implements query.Table and query.ColReader.
type queryTable struct {
	row      *Row
	groupKey query.GroupKey
	colMeta  []query.ColMeta
	cols     []interface{}
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

// Data in a column is laid out in the following way:
//   [ r.row.Columns... , r.tagKeys()... , r.row.Name ]
func (t *queryTable) translateRowsToColumns() error {
	cols := t.Cols()
	t.cols = make([]interface{}, len(cols))
	for i, col := range cols {
		switch col.Type {
		case query.TFloat:
			t.cols[i] = make([]float64, 0, t.Len())
		case query.TInt:
			t.cols[i] = make([]int64, 0, t.Len())
		case query.TUInt:
			t.cols[i] = make([]uint64, 0, t.Len())
		case query.TString:
			t.cols[i] = make([]string, 0, t.Len())
		case query.TBool:
			t.cols[i] = make([]bool, 0, t.Len())
		case query.TTime:
			t.cols[i] = make([]values.Time, 0, t.Len())
		}
	}
	for _, els := range t.row.Values {
		for i, el := range els {
			col := cols[i]
			switch col.Type {
			case query.TFloat:
				val, ok := el.(float64)
				if !ok {
					return fmt.Errorf("unsupported type %T found in column %s of type %s", val, col.Label, col.Type)
				}
				t.cols[i] = append(t.cols[i].([]float64), val)
			case query.TInt:
				val, ok := el.(int64)
				if !ok {
					return fmt.Errorf("unsupported type %T found in column %s of type %s", val, col.Label, col.Type)
				}
				t.cols[i] = append(t.cols[i].([]int64), val)
			case query.TUInt:
				val, ok := el.(uint64)
				if !ok {
					return fmt.Errorf("unsupported type %T found in column %s of type %s", val, col.Label, col.Type)
				}
				t.cols[i] = append(t.cols[i].([]uint64), val)
			case query.TString:
				val, ok := el.(string)
				if !ok {
					return fmt.Errorf("unsupported type %T found in column %s of type %s", val, col.Label, col.Type)
				}
				t.cols[i] = append(t.cols[i].([]string), val)
			case query.TBool:
				val, ok := el.(bool)
				if !ok {
					return fmt.Errorf("unsupported type %T found in column %s of type %s", val, col.Label, col.Type)
				}
				t.cols[i] = append(t.cols[i].([]bool), val)
			case query.TTime:
				switch val := el.(type) {
				case int64:
					t.cols[i] = append(t.cols[i].([]values.Time), values.Time(val))
				case float64:
					t.cols[i] = append(t.cols[i].([]values.Time), values.Time(val))
				case string:
					tm, err := time.Parse(time.RFC3339, val)
					if err != nil {
						return fmt.Errorf("could not parse string %q as time: %v", val, err)
					}
					t.cols[i] = append(t.cols[i].([]values.Time), values.ConvertTime(tm))
				default:
					return fmt.Errorf("unsupported type %T found in column %s", val, col.Label)
				}
			default:
				return fmt.Errorf("invalid type %T found in column %s", el, col.Label)
			}
		}

		j := len(t.row.Columns)
		for j < len(t.row.Columns)+len(t.row.Tags) {
			col := cols[j]
			t.cols[j] = append(t.cols[j].([]string), t.row.Tags[col.Label])
			j++
		}

		t.cols[j] = append(t.cols[j].([]string), t.row.Name)
	}

	return nil
}

// Key constructs the query.GroupKey for a Row from the rows
// tags and measurement.
// It is used to implement query.Table and query.ColReader.
func (r *queryTable) Key() query.GroupKey {
	if r.groupKey == nil {
		cols := make([]query.ColMeta, len(r.row.Tags)+1) // plus one is for measurement
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
			v, err := values.NewValue(kvs[j], execute.ConvertToKind(cols[j].Type))
			if err != nil {
				panic(err)
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
// It is used to implement query.Table and query.ColReader.
func (r *queryTable) Cols() []query.ColMeta {
	if r.colMeta == nil {
		colMeta := make([]query.ColMeta, len(r.row.Columns)+len(r.row.Tags)+1)
		for i, col := range r.row.Columns {
			colMeta[i] = query.ColMeta{
				Label: col,
				Type:  query.TInvalid,
			}
			if col == "time" {
				// rename the time column
				colMeta[i].Label = "_time"
				colMeta[i].Type = query.TTime
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
				colMeta[i].Type = query.TFloat
			case int64:
				colMeta[i].Type = query.TInt
			case uint64:
				colMeta[i].Type = query.TUInt
			case bool:
				colMeta[i].Type = query.TBool
			case string:
				colMeta[i].Type = query.TString
			}
		}

		tags := r.tagKeys()

		leng := len(r.row.Columns)
		for i, tag := range tags {
			colMeta[leng+i] = query.ColMeta{
				Label: tag,
				Type:  query.TString,
			}
		}

		leng = leng + len(tags)
		colMeta[leng] = query.ColMeta{
			Label: "_measurement",
			Type:  query.TString,
		}
		r.colMeta = colMeta
	}

	return r.colMeta
}

// Do applies f to itself. This is because Row is a query.ColReader.
// It is used to implement query.Table.
func (r *queryTable) Do(f func(query.ColReader) error) error {
	return f(r)
}

// RefCount is a noop.
// It is used to implement query.ColReader.
func (r *queryTable) RefCount(n int) {}

// Empty returns true if a Row has no values.
// It is used to implement query.Table.
func (r *queryTable) Empty() bool { return r.Len() == 0 }

// Len returns the length or r.row.Values
// It is used to implement query.ColReader.
func (r *queryTable) Len() int {
	return len(r.row.Values)
}

// Bools returns the values in column index j as bools.
// It will panic if the column is not a []bool.
// It is used to implement query.ColReader.
func (r *queryTable) Bools(j int) []bool {
	return r.cols[j].([]bool)
}

// Ints returns the values in column index j as ints.
// It will panic if the column is not a []int64.
// It is used to implement query.ColReader.
func (r *queryTable) Ints(j int) []int64 {
	return r.cols[j].([]int64)
}

// UInts returns the values in column index j as ints.
// It will panic if the column is not a []uint64.
// It is used to implement query.ColReader.
func (r *queryTable) UInts(j int) []uint64 {
	return r.cols[j].([]uint64)
}

// Floats returns the values in column index j as floats.
// It will panic if the column is not a []float64.
// It is used to implement query.ColReader.
func (r *queryTable) Floats(j int) []float64 {
	return r.cols[j].([]float64)
}

// Strings returns the values in column index j as strings.
// It will panic if the column is not a []string.
// It is used to implement query.ColReader.
func (r *queryTable) Strings(j int) []string {
	return r.cols[j].([]string)
}

// Times returns the values in column index j as values.Times.
// It will panic if the column is not a []values.Time.
// It is used to implement query.ColReader.
func (r *queryTable) Times(j int) []values.Time {
	return r.cols[j].([]values.Time)
}
