package executetest

import (
	"fmt"

	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/values"
)

// Table is an implementation of execute.Table
// It is designed to make it easy to statically declare the data within the table.
// Not all fields need to be set. See comments on each field.
// Use Normalize to ensure that all fields are set before equality comparisons.
type Table struct {
	// GroupKey of the table. Does not need to be set explicitly.
	GroupKey query.GroupKey
	// KeyCols is a list of column that are part of the group key.
	// The column type is deduced from the ColMeta slice.
	KeyCols []string
	// KeyValues is a list of values for the group key columns.
	// Only needs to be set when no data is present on the table.
	KeyValues []interface{}
	// ColMeta is a list of columns of the table.
	ColMeta []query.ColMeta
	// Data is a list of rows, i.e. Data[row][col]
	// Each row must be a list with length equal to len(ColMeta)
	Data [][]interface{}
}

// Normalize ensures all fields of the table are set correctly.
func (t *Table) Normalize() {
	if t.GroupKey == nil {
		cols := make([]query.ColMeta, len(t.KeyCols))
		vs := make([]values.Value, len(t.KeyCols))
		if len(t.KeyValues) != len(t.KeyCols) {
			t.KeyValues = make([]interface{}, len(t.KeyCols))
		}
		for j, label := range t.KeyCols {
			idx := execute.ColIdx(label, t.ColMeta)
			if idx < 0 {
				panic(fmt.Errorf("table invalid: missing group column %q", label))
			}
			cols[j] = t.ColMeta[idx]
			if len(t.Data) > 0 {
				t.KeyValues[j] = t.Data[0][idx]
			}
			v, err := values.NewValue(t.KeyValues[j], execute.ConvertToKind(cols[j].Type))
			if err != nil {
				panic(err)
			}
			vs[j] = v
		}
		t.GroupKey = execute.NewGroupKey(cols, vs)
	}
}

func (t *Table) Empty() bool {
	return len(t.Data) == 0
}

func (t *Table) RefCount(n int) {}

func (t *Table) Cols() []query.ColMeta {
	return t.ColMeta
}

func (t *Table) Key() query.GroupKey {
	t.Normalize()
	return t.GroupKey
}

func (t *Table) Do(f func(query.ColReader) error) error {
	for _, r := range t.Data {
		if err := f(ColReader{
			key:  t.Key(),
			cols: t.ColMeta,
			row:  r,
		}); err != nil {
			return err
		}
	}
	return nil
}

type ColReader struct {
	key  query.GroupKey
	cols []query.ColMeta
	row  []interface{}
}

func (cr ColReader) Cols() []query.ColMeta {
	return cr.cols
}

func (cr ColReader) Key() query.GroupKey {
	return cr.key
}
func (cr ColReader) Len() int {
	return 1
}

func (cr ColReader) Bools(j int) []bool {
	return []bool{cr.row[j].(bool)}
}

func (cr ColReader) Ints(j int) []int64 {
	return []int64{cr.row[j].(int64)}
}

func (cr ColReader) UInts(j int) []uint64 {
	return []uint64{cr.row[j].(uint64)}
}

func (cr ColReader) Floats(j int) []float64 {
	return []float64{cr.row[j].(float64)}
}

func (cr ColReader) Strings(j int) []string {
	return []string{cr.row[j].(string)}
}

func (cr ColReader) Times(j int) []execute.Time {
	return []execute.Time{cr.row[j].(execute.Time)}
}

func TablesFromCache(c execute.DataCache) (tables []*Table, err error) {
	c.ForEach(func(key query.GroupKey) {
		if err != nil {
			return
		}
		var b query.Table
		b, err = c.Table(key)
		if err != nil {
			return
		}
		var cb *Table
		cb, err = ConvertTable(b)
		if err != nil {
			return
		}
		tables = append(tables, cb)
	})
	return tables, nil
}

func ConvertTable(b query.Table) (*Table, error) {
	key := b.Key()
	blk := &Table{
		GroupKey: key,
		ColMeta:  b.Cols(),
	}

	keyCols := key.Cols()
	if len(keyCols) > 0 {
		blk.KeyCols = make([]string, len(keyCols))
		blk.KeyValues = make([]interface{}, len(keyCols))
		for j, c := range keyCols {
			blk.KeyCols[j] = c.Label
			var v interface{}
			switch c.Type {
			case query.TBool:
				v = key.ValueBool(j)
			case query.TUInt:
				v = key.ValueUInt(j)
			case query.TInt:
				v = key.ValueInt(j)
			case query.TFloat:
				v = key.ValueFloat(j)
			case query.TString:
				v = key.ValueString(j)
			case query.TTime:
				v = key.ValueTime(j)
			default:
				return nil, fmt.Errorf("unsupported column type %v", c.Type)
			}
			blk.KeyValues[j] = v
		}
	}

	err := b.Do(func(cr query.ColReader) error {
		l := cr.Len()
		for i := 0; i < l; i++ {
			row := make([]interface{}, len(blk.ColMeta))
			for j, c := range blk.ColMeta {
				var v interface{}
				switch c.Type {
				case query.TBool:
					v = cr.Bools(j)[i]
				case query.TInt:
					v = cr.Ints(j)[i]
				case query.TUInt:
					v = cr.UInts(j)[i]
				case query.TFloat:
					v = cr.Floats(j)[i]
				case query.TString:
					v = cr.Strings(j)[i]
				case query.TTime:
					v = cr.Times(j)[i]
				default:
					panic(fmt.Errorf("unknown column type %s", c.Type))
				}
				row[j] = v
			}
			blk.Data = append(blk.Data, row)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return blk, nil
}

type SortedTables []*Table

func (b SortedTables) Len() int {
	return len(b)
}

func (b SortedTables) Less(i int, j int) bool {
	return b[i].Key().Less(b[j].Key())
}

func (b SortedTables) Swap(i int, j int) {
	b[i], b[j] = b[j], b[i]
}

// NormalizeTables ensures that each table is normalized
func NormalizeTables(bs []*Table) {
	for _, b := range bs {
		b.Key()
	}
}
