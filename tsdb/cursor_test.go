package tsdb_test

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/influxdb/influxdb/tsdb"
)

// Ensure the multi-cursor can correctly iterate across a single subcursor.
func TestMultiCursor_Single(t *testing.T) {
	mc := tsdb.MultiCursor(
		NewCursor([]CursorItem{
			{Key: []byte{0x00}, Value: []byte{0x00}},
			{Key: []byte{0x01}, Value: []byte{0x10}},
			{Key: []byte{0x02}, Value: []byte{0x20}},
		}),
	)

	if k, v := mc.Seek([]byte{0x00}); !bytes.Equal(k, []byte{0x00}) || !bytes.Equal(v, []byte{0x00}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); !bytes.Equal(k, []byte{0x01}) || !bytes.Equal(v, []byte{0x10}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); !bytes.Equal(k, []byte{0x02}) || !bytes.Equal(v, []byte{0x20}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != nil {
		t.Fatalf("expected eof, got: %x / %x", k, v)
	}
}

// Ensure the multi-cursor can correctly iterate across multiple non-overlapping subcursors.
func TestMultiCursor_Multiple_NonOverlapping(t *testing.T) {
	mc := tsdb.MultiCursor(
		NewCursor([]CursorItem{
			{Key: []byte{0x00}, Value: []byte{0x00}},
			{Key: []byte{0x03}, Value: []byte{0x30}},
			{Key: []byte{0x04}, Value: []byte{0x40}},
		}),
		NewCursor([]CursorItem{
			{Key: []byte{0x01}, Value: []byte{0x10}},
			{Key: []byte{0x02}, Value: []byte{0x20}},
		}),
	)

	if k, v := mc.Seek([]byte{0x00}); !bytes.Equal(k, []byte{0x00}) || !bytes.Equal(v, []byte{0x00}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); !bytes.Equal(k, []byte{0x01}) || !bytes.Equal(v, []byte{0x10}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); !bytes.Equal(k, []byte{0x02}) || !bytes.Equal(v, []byte{0x20}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); !bytes.Equal(k, []byte{0x03}) || !bytes.Equal(v, []byte{0x30}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); !bytes.Equal(k, []byte{0x04}) || !bytes.Equal(v, []byte{0x40}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != nil {
		t.Fatalf("expected eof, got: %x / %x", k, v)
	}
}

// Ensure the multi-cursor can correctly iterate across multiple overlapping subcursors.
func TestMultiCursor_Multiple_Overlapping(t *testing.T) {
	mc := tsdb.MultiCursor(
		NewCursor([]CursorItem{
			{Key: []byte{0x00}, Value: []byte{0x00}},
			{Key: []byte{0x03}, Value: []byte{0x03}},
			{Key: []byte{0x04}, Value: []byte{0x04}},
		}),
		NewCursor([]CursorItem{
			{Key: []byte{0x00}, Value: []byte{0xF0}},
			{Key: []byte{0x02}, Value: []byte{0xF2}},
			{Key: []byte{0x04}, Value: []byte{0xF4}},
		}),
	)

	if k, v := mc.Seek([]byte{0x00}); !bytes.Equal(k, []byte{0x00}) || !bytes.Equal(v, []byte{0x00}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); !bytes.Equal(k, []byte{0x02}) || !bytes.Equal(v, []byte{0xF2}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); !bytes.Equal(k, []byte{0x03}) || !bytes.Equal(v, []byte{0x03}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); !bytes.Equal(k, []byte{0x04}) || !bytes.Equal(v, []byte{0x04}) {
		t.Fatalf("unexpected key/value: %x / %x", k, v)
	} else if k, v = mc.Next(); k != nil {
		t.Fatalf("expected eof, got: %x / %x", k, v)
	}
}

// Ensure the multi-cursor can handle randomly generated data.
func TestMultiCursor_Quick(t *testing.T) {
	quick.Check(func(seek uint64, cursors []Cursor) bool {
		var got, exp [][]byte
		seek %= 100

		// Merge all cursor data to determine expected output.
		// First seen key overrides all other items with the same key.
		m := make(map[string][]byte)
		for _, c := range cursors {
			for _, item := range c.items {
				if bytes.Compare(item.Key, u64tob(seek)) == -1 {
					continue
				}
				if _, ok := m[string(item.Key)]; ok {
					continue
				}
				m[string(item.Key)] = item.Value
			}
		}

		// Convert map back to single item list.
		for k, v := range m {
			exp = append(exp, append([]byte(k), v...))
		}
		sort.Sort(byteSlices(exp))

		// Create multi-cursor and iterate over all items.
		mc := tsdb.MultiCursor(tsdbCursorSlice(cursors)...)
		for k, v := mc.Seek(u64tob(seek)); k != nil; k, v = mc.Next() {
			got = append(got, append(k, v...))
		}

		// Verify results.
		if !reflect.DeepEqual(got, exp) {
			t.Fatalf("mismatch: seek=%d\n\ngot=%+v\n\nexp=%+v", seek, got, exp)
		}

		return true
	}, nil)
}

// Cursor represents an in-memory test cursor.
type Cursor struct {
	items []CursorItem
	index int
}

// NewCursor returns a new instance of Cursor.
func NewCursor(items []CursorItem) *Cursor {
	sort.Sort(CursorItems(items))
	return &Cursor{items: items}
}

// Seek seeks to an item by key.
func (c *Cursor) Seek(seek []byte) (key, value []byte) {
	for c.index = 0; c.index < len(c.items); c.index++ {
		if bytes.Compare(c.items[c.index].Key, seek) == -1 { // skip keys less than seek
			continue
		}
		return c.items[c.index].Key, c.items[c.index].Value
	}
	return nil, nil
}

// Next returns the next key/value pair.
func (c *Cursor) Next() (key, value []byte) {
	if c.index >= len(c.items)-1 {
		return nil, nil
	}

	c.index++
	return c.items[c.index].Key, c.items[c.index].Value
}

// Generate returns a randomly generated cursor. Implements quick.Generator.
func (c Cursor) Generate(rand *rand.Rand, size int) reflect.Value {
	c.index = 0

	c.items = make([]CursorItem, rand.Intn(size))
	for i := range c.items {
		value, _ := quick.Value(reflect.TypeOf([]byte(nil)), rand)

		c.items[i] = CursorItem{
			Key:   u64tob(uint64(rand.Intn(size))),
			Value: value.Interface().([]byte),
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
	Key   []byte
	Value []byte
}

type CursorItems []CursorItem

func (a CursorItems) Len() int           { return len(a) }
func (a CursorItems) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a CursorItems) Less(i, j int) bool { return bytes.Compare(a[i].Key, a[j].Key) == -1 }

// byteSlices represents a sortable slice of byte slices.
type byteSlices [][]byte

func (a byteSlices) Len() int           { return len(a) }
func (a byteSlices) Less(i, j int) bool { return bytes.Compare(a[i], a[j]) == -1 }
func (a byteSlices) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// u64tob converts a uint64 into an 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
