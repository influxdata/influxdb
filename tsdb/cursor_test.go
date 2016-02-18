package tsdb_test

import (
	"bytes"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/davecgh/go-spew/spew"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/pkg/deep"
	"github.com/influxdata/influxdb/tsdb"
)

// Ensure the multi-cursor can correctly iterate across a single subcursor.
func TestMultiCursor_Single(t *testing.T) {
	mc := tsdb.MultiCursor(NewCursor([]CursorItem{
		{Key: 0, Value: 0},
		{Key: 1, Value: 10},
		{Key: 2, Value: 20},
	}, true))

	if k, v := mc.SeekTo(0); k != 0 || v.(int) != 0 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 1 || v.(int) != 10 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 2 || v.(int) != 20 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != tsdb.EOF {
		t.Fatalf("expected eof, got: %x / %x", k, v)
	}
}

// Ensure the multi-cursor can correctly iterate across a single subcursor in reverse order.
func TestMultiCursor_Single_Reverse(t *testing.T) {
	mc := tsdb.MultiCursor(NewCursor([]CursorItem{
		{Key: 0, Value: 0},
		{Key: 1, Value: 10},
		{Key: 2, Value: 20},
	}, false))

	if k, v := mc.SeekTo(2); k != 2 || v.(int) != 20 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 1 || v.(int) != 10 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 0 || v.(int) != 0 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != tsdb.EOF {
		t.Fatalf("expected eof, got: %x / %x", k, v)
	}
}

// Ensure the multi-cursor can correctly iterate across multiple non-overlapping subcursors.
func TestMultiCursor_Multiple_NonOverlapping(t *testing.T) {
	mc := tsdb.MultiCursor(
		NewCursor([]CursorItem{
			{Key: 0, Value: 0},
			{Key: 3, Value: 30},
			{Key: 4, Value: 40},
		}, true),
		NewCursor([]CursorItem{
			{Key: 1, Value: 10},
			{Key: 2, Value: 20},
		}, true),
	)

	if k, v := mc.SeekTo(0); k != 0 || v.(int) != 0 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 1 || v.(int) != 10 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 2 || v.(int) != 20 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 3 || v.(int) != 30 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 4 || v.(int) != 40 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != tsdb.EOF {
		t.Fatalf("expected eof, got: %x / %x", k, v)
	}
}

// Ensure the multi-cursor can correctly iterate across multiple non-overlapping subcursors.
func TestMultiCursor_Multiple_NonOverlapping_Reverse(t *testing.T) {
	mc := tsdb.MultiCursor(
		NewCursor([]CursorItem{
			{Key: 0, Value: 0},
			{Key: 3, Value: 30},
			{Key: 4, Value: 40},
		}, false),
		NewCursor([]CursorItem{
			{Key: 1, Value: 10},
			{Key: 2, Value: 20},
		}, false),
	)

	if k, v := mc.SeekTo(4); k != 4 || v.(int) != 40 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 3 || v.(int) != 30 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 2 || v.(int) != 20 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 1 || v.(int) != 10 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 0 || v.(int) != 00 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != tsdb.EOF {
		t.Fatalf("expected eof, got: %x / %x", k, v)
	}
}

// Ensure the multi-cursor can correctly iterate across multiple overlapping subcursors.
func TestMultiCursor_Multiple_Overlapping(t *testing.T) {
	mc := tsdb.MultiCursor(
		NewCursor([]CursorItem{
			{Key: 0, Value: 0},
			{Key: 3, Value: 3},
			{Key: 4, Value: 4},
		}, true),
		NewCursor([]CursorItem{
			{Key: 0, Value: 0xF0},
			{Key: 2, Value: 0xF2},
			{Key: 4, Value: 0xF4},
		}, true),
	)

	if k, v := mc.SeekTo(0); k != 0 || v.(int) != 0 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 2 || v.(int) != 0xF2 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 3 || v.(int) != 3 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 4 || v.(int) != 4 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != tsdb.EOF {
		t.Fatalf("expected eof, got: %x / %x", k, v)
	}
}

// Ensure the multi-cursor can correctly iterate across multiple overlapping subcursors.
func TestMultiCursor_Multiple_Overlapping_Reverse(t *testing.T) {
	mc := tsdb.MultiCursor(
		NewCursor([]CursorItem{
			{Key: 0, Value: 0},
			{Key: 3, Value: 3},
			{Key: 4, Value: 4},
		}, false),
		NewCursor([]CursorItem{
			{Key: 0, Value: 0xF0},
			{Key: 2, Value: 0xF2},
			{Key: 4, Value: 0xF4},
		}, false),
	)

	if k, v := mc.SeekTo(4); k != 4 || v.(int) != 4 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 3 || v.(int) != 3 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 2 || v.(int) != 0xF2 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != 0 || v.(int) != 0 {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != tsdb.EOF {
		t.Fatalf("expected eof, got: %x / %x", k, v)
	}
}

// Ensure the multi-cursor can handle randomly generated data.
func TestMultiCursor_Quick(t *testing.T) {
	quick.Check(func(useek uint64, cursors []Cursor) bool {
		var got, exp []CursorItem
		seek := int64(useek) % 100

		// Merge all cursor data to determine expected output.
		// First seen key overrides all other items with the same key.
		m := make(map[int64]CursorItem)
		for _, c := range cursors {
			for _, item := range c.items {
				if item.Key < seek {
					continue
				}
				if _, ok := m[item.Key]; ok {
					continue
				}
				m[item.Key] = item
			}
		}

		// Convert map back to single item list.
		for _, item := range m {
			exp = append(exp, item)
		}
		sort.Sort(CursorItems(exp))

		// Create multi-cursor and iterate over all items.
		mc := tsdb.MultiCursor(tsdbCursorSlice(cursors)...)
		for k, v := mc.SeekTo(seek); k != tsdb.EOF; k, v = mc.Next() {
			got = append(got, CursorItem{k, v.(int)})
		}

		// Verify results.
		if !reflect.DeepEqual(got, exp) {
			t.Fatalf("mismatch: seek=%d\n\ngot=%+v\n\nexp=%+v", seek, got, exp)
		}

		return true
	}, nil)
}

// Ensure a cursor with a single ref value can be converted into an iterator.
func TestFloatCursorIterator_SingleValue(t *testing.T) {
	cur := NewCursor([]CursorItem{
		{Key: 0, Value: float64(100)},
		{Key: 3, Value: float64(200)},
	}, true)

	opt := influxql.IteratorOptions{
		Expr:      &influxql.VarRef{Val: "value"},
		Ascending: true,
		StartTime: influxql.MinTime,
		EndTime:   influxql.MaxTime,
	}
	itr := tsdb.NewFloatCursorIterator("series0", map[string]string{"host": "serverA"}, cur, opt)
	defer itr.Close()

	if p := itr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "series0",
		Time:  0,
		Value: float64(100),
	}) {
		t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
	}

	if p := itr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "series0",
		Time:  3,
		Value: float64(200),
	}) {
		t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
	}

	if p := itr.Next(); p != nil {
		t.Fatalf("expected eof, got: %s", spew.Sdump(p))
	}
}

// Ensure a cursor with a ref and multiple aux values can be converted into an iterator.
func TestFloatCursorIterator_MultipleValues(t *testing.T) {
	cur := NewCursor([]CursorItem{
		{Key: 0, Value: map[string]interface{}{"val1": float64(100), "val2": "foo"}},
		{Key: 3, Value: map[string]interface{}{"val1": float64(200), "val2": "bar"}},
	}, true)

	opt := influxql.IteratorOptions{
		Expr: &influxql.VarRef{Val: "val1"}, Aux: []string{"val1", "val2"},
		Ascending: true,
		StartTime: influxql.MinTime,
		EndTime:   influxql.MaxTime,
	}
	itr := tsdb.NewFloatCursorIterator("series0", map[string]string{"host": "serverA"}, cur, opt)
	defer itr.Close()

	if p := itr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "series0",
		Time:  0,
		Value: 100,
		Aux:   []interface{}{float64(100), "foo"},
	}) {
		t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
	}

	if p := itr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "series0",
		Time:  3,
		Value: 200,
		Aux:   []interface{}{float64(200), "bar"},
	}) {
		t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
	}

	if p := itr.Next(); p != nil {
		t.Fatalf("expected eof, got: %s", spew.Sdump(p))
	}
}

// Ensure a cursor with a single value can be converted into an iterator.
func TestFloatCursorIterator_Aux_SingleValue(t *testing.T) {
	cur := NewCursor([]CursorItem{
		{Key: 0, Value: float64(100)},
		{Key: 3, Value: float64(200)},
	}, true)

	opt := influxql.IteratorOptions{
		Aux:       []string{"val1"},
		Ascending: true,
		StartTime: influxql.MinTime,
		EndTime:   influxql.MaxTime,
	}
	itr := tsdb.NewFloatCursorIterator("series0", map[string]string{"host": "serverA"}, cur, opt)
	defer itr.Close()

	if p := itr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "series0",
		Time:  0,
		Value: math.NaN(),
		Aux:   []interface{}{float64(100)},
	}) {
		t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
	}

	if p := itr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "series0",
		Time:  3,
		Value: math.NaN(),
		Aux:   []interface{}{float64(200)},
	}) {
		t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
	}

	if p := itr.Next(); p != nil {
		t.Fatalf("expected eof, got: %s", spew.Sdump(p))
	}
}

// Ensure a cursor with multiple values can be converted into an iterator.
func TestFloatCursorIterator_Aux_MultipleValues(t *testing.T) {
	cur := NewCursor([]CursorItem{
		{Key: 0, Value: map[string]interface{}{"val1": float64(100), "val2": "foo"}},
		{Key: 3, Value: map[string]interface{}{"val1": float64(200), "val2": "bar"}},
	}, true)

	opt := influxql.IteratorOptions{
		Aux:       []string{"val1", "val2"},
		Ascending: true,
		StartTime: influxql.MinTime,
		EndTime:   influxql.MaxTime,
	}
	itr := tsdb.NewFloatCursorIterator("series0", map[string]string{"host": "serverA"}, cur, opt)
	defer itr.Close()

	if p := itr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "series0",
		Time:  0,
		Value: math.NaN(),
		Aux:   []interface{}{float64(100), "foo"},
	}) {
		t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
	}

	if p := itr.Next(); !deep.Equal(p, &influxql.FloatPoint{
		Name:  "series0",
		Time:  3,
		Value: math.NaN(),
		Aux:   []interface{}{float64(200), "bar"},
	}) {
		t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
	}

	if p := itr.Next(); p != nil {
		t.Fatalf("expected eof, got: %s", spew.Sdump(p))
	}
}

// Ensure a cursor iterator does not go past the end time.
func TestFloatCursorIterator_EndTime(t *testing.T) {
	cur := NewCursor([]CursorItem{
		{Key: 0, Value: float64(100)},
		{Key: 3, Value: float64(200)},
		{Key: 4, Value: float64(300)},
	}, true)

	itr := tsdb.NewFloatCursorIterator("x", nil, cur, influxql.IteratorOptions{
		Expr:      &influxql.VarRef{Val: "value"},
		Ascending: true,
		EndTime:   3,
	})
	defer itr.Close()

	// Verify that only two points are emitted.
	if p := itr.Next(); p == nil || p.Time != 0 {
		t.Fatalf("unexpected point(0): %s", spew.Sdump(p))
	}
	if p := itr.Next(); p == nil || p.Time != 3 {
		t.Fatalf("unexpected point(1): %s", spew.Sdump(p))
	}
	if p := itr.Next(); p != nil {
		t.Fatalf("expected eof, got: %s", spew.Sdump(p))
	}
}

// Cursor represents an in-memory test cursor.
type Cursor struct {
	items     []CursorItem
	index     int
	ascending bool
}

// NewCursor returns a new instance of Cursor.
func NewCursor(items []CursorItem, ascending bool) *Cursor {
	index := 0
	sort.Sort(CursorItems(items))

	if !ascending {
		index = len(items)
	}
	return &Cursor{
		items:     items,
		index:     index,
		ascending: ascending,
	}
}

func (c *Cursor) Ascending() bool { return c.ascending }

// Seek seeks to an item by key.
func (c *Cursor) SeekTo(seek int64) (key int64, value interface{}) {
	if c.ascending {
		return c.seekForward(seek)
	}
	return c.seekReverse(seek)
}

func (c *Cursor) seekForward(seek int64) (key int64, value interface{}) {
	for c.index = 0; c.index < len(c.items); c.index++ {
		if c.items[c.index].Key < seek { // skip keys less than seek
			continue
		}
		key, value = c.items[c.index].Key, c.items[c.index].Value
		c.index++
		return key, value
	}
	return tsdb.EOF, nil
}

func (c *Cursor) seekReverse(seek int64) (key int64, value interface{}) {
	for c.index = len(c.items) - 1; c.index >= 0; c.index-- {
		if c.items[c.index].Key > seek { // skip keys greater than seek
			continue
		}
		key, value = c.items[c.index].Key, c.items[c.index].Value
		c.index--
		return key, value
	}
	return tsdb.EOF, nil
}

// Next returns the next key/value pair.
func (c *Cursor) Next() (key int64, value interface{}) {
	if !c.ascending && c.index < 0 {
		return tsdb.EOF, nil
	}

	if c.ascending && c.index >= len(c.items) {
		return tsdb.EOF, nil
	}

	k, v := c.items[c.index].Key, c.items[c.index].Value

	if c.ascending {
		c.index++
	} else {
		c.index--
	}
	return k, v
}

// Generate returns a randomly generated cursor. Implements quick.Generator.
func (c Cursor) Generate(rand *rand.Rand, size int) reflect.Value {
	c.index = 0
	c.ascending = true

	c.items = make([]CursorItem, rand.Intn(size))
	for i := range c.items {
		c.items[i] = CursorItem{
			Key:   rand.Int63n(int64(size)),
			Value: rand.Int(),
		}
	}

	// Sort items by key.
	sort.Sort(CursorItems(c.items))

	return reflect.ValueOf(c)
}

// tsdbCursorSlice converts a Cursor slice to a tsdb.Cursor slice.
func tsdbCursorSlice(a []Cursor) []tsdb.Cursor {
	var other []tsdb.Cursor
	for i := range a {
		other = append(other, &a[i])
	}
	return other
}

// CursorItem represents a key/value pair in a cursor.
type CursorItem struct {
	Key   int64
	Value interface{}
}

type CursorItems []CursorItem

func (a CursorItems) Len() int           { return len(a) }
func (a CursorItems) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a CursorItems) Less(i, j int) bool { return a[i].Key < a[j].Key }

// byteSlices represents a sortable slice of byte slices.
type byteSlices [][]byte

func (a byteSlices) Len() int           { return len(a) }
func (a byteSlices) Less(i, j int) bool { return bytes.Compare(a[i], a[j]) == -1 }
func (a byteSlices) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
