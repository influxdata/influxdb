package tsm1_test

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"
	"time"

	"github.com/influxdb/influxdb/tsdb"
	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
)

func TestCombinedEngineCursor_Quick(t *testing.T) {
	const tmin = 0
	quick.Check(func(wc, ec *Cursor, ascending bool, seek int64) bool {
		c := tsm1.NewCombinedEngineCursor(wc, ec, ascending)
		// Read from cursor.
		got := make([]int64, 0)
		for k, _ := c.SeekTo(seek); k != tsdb.EOF; k, _ = c.Next() {
			got = append(got, k)
		}
		// Merge cursors items.
		merged := MergeCursorItems(wc.items, ec.items)
		if !ascending {
			sort.Sort(sort.Reverse(CursorItems(merged)))
		}
		// Filter out items outside of seek range.
		exp := make([]int64, 0)
		for _, item := range merged {
			if (ascending && item.Key < seek) || (!ascending && item.Key > seek) {
				continue
			}
			exp = append(exp, item.Key)
		}
		if !reflect.DeepEqual(got, exp) {
			t.Fatalf("mismatch: seek=%v, ascending=%v\n\ngot=%#v\n\nexp=%#v\n\n", seek, ascending, got, exp)
		}
		return true
	}, &quick.Config{Values: func(values []reflect.Value, rand *rand.Rand) {
		ascending := rand.Intn(1) == 1
		values[0] = reflect.ValueOf(GenerateCursor(tmin, 10, ascending, rand))
		values[1] = reflect.ValueOf(GenerateCursor(tmin, 10, ascending, rand))
		values[2] = reflect.ValueOf(ascending)
		values[3] = reflect.ValueOf(rand.Int63n(100))
	}})
}

// Cursor represents a simple test cursor that implements tsdb.Cursor.
type Cursor struct {
	i         int
	items     []CursorItem
	ascending bool
}

// NewCursor returns a new instance of Cursor.
func NewCursor(items []CursorItem, ascending bool) *Cursor {
	c := &Cursor{
		items:     items,
		ascending: ascending,
	}
	// Set initial position depending on cursor direction.
	if ascending {
		c.i = -1
	} else {
		c.i = len(c.items)
	}
	return c
}

// CursorItem represents an item in a test cursor.
type CursorItem struct {
	Key   int64
	Value interface{}
}

// SeekTo moves the cursor to the first key greater than or equal to seek.
func (c *Cursor) SeekTo(seek int64) (key int64, value interface{}) {
	if c.ascending {
		for i, item := range c.items {
			if item.Key >= seek {
				c.i = i
				return item.Key, item.Value
			}
		}
	} else {
		for i := len(c.items) - 1; i >= 0; i-- {
			if item := c.items[i]; item.Key <= seek {
				c.i = i
				return item.Key, item.Value
			}
		}
	}
	c.i = len(c.items)
	return tsdb.EOF, nil
}

// Next returns the next key/value from the cursor.
func (c *Cursor) Next() (key int64, value interface{}) {
	if c.ascending {
		c.i++
		if c.i >= len(c.items) {
			return tsdb.EOF, nil
		}
	} else if !c.ascending {
		c.i--
		if c.i < 0 {
			return tsdb.EOF, nil
		}
	}
	return c.items[c.i].Key, c.items[c.i].Value
}

// Ascending returns true if the cursor moves in ascending order.
func (c *Cursor) Ascending() bool { return c.ascending }

// CursorItems represents a list of CursorItem objects.
type CursorItems []CursorItem

func (a CursorItems) Len() int           { return len(a) }
func (a CursorItems) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a CursorItems) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Keys returns a list of keys.
func (a CursorItems) Keys() []int64 {
	keys := make([]int64, len(a))
	for i := range a {
		keys[i] = a[i].Key
	}
	return keys
}

// GenerateCursor generates a cursor with a random data.
func GenerateCursor(tmin, step int64, ascending bool, rand *rand.Rand) *Cursor {
	key := tmin + rand.Int63n(10)
	items := make([]CursorItem, 0)
	for i, n := 0, rand.Intn(100); i < n; i++ {
		items = append(items, CursorItem{
			Key:   key,
			Value: int64(0),
		})
		key += rand.Int63n(10)
	}
	return NewCursor(items, ascending)
}

// MergeCursorItems merges items in a & b together.
// If two items share a timestamp then a takes precendence.
func MergeCursorItems(a, b []CursorItem) []CursorItem {
	items := make([]CursorItem, 0)
	var ai, bi int
	for {
		if ai < len(a) && bi < len(b) {
			if ak, bk := a[ai].Key, b[bi].Key; ak == bk {
				items = append(items, a[ai])
				ai++
				bi++
			} else if ak < bk {
				items = append(items, a[ai])
				ai++
			} else {
				items = append(items, b[bi])
				bi++
			}
		} else if ai < len(a) {
			items = append(items, a[ai])
			ai++
		} else if bi < len(b) {
			items = append(items, b[bi])
			bi++
		} else {
			break
		}
	}
	return items
}

// ReadAllCursor slurps all values from a cursor.
func ReadAllCursor(c tsdb.Cursor) tsm1.Values {
	var values tsm1.Values
	for k, v := c.Next(); k != tsdb.EOF; k, v = c.Next() {
		values = append(values, tsm1.NewValue(time.Unix(0, k).UTC(), v))
	}
	return values
}

// DedupeValues returns a list of values with duplicate times removed.
func DedupeValues(a tsm1.Values) tsm1.Values {
	other := make(tsm1.Values, 0, len(a))
	m := map[int64]struct{}{}

	for i := len(a) - 1; i >= 0; i-- {
		value := a[i]
		if _, ok := m[value.UnixNano()]; ok {
			continue
		}

		other = append(other, value)
		m[value.UnixNano()] = struct{}{}
	}

	return other
}
