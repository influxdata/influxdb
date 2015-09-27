package pd1

import (
	"sort"
	"time"

	"github.com/dgryski/go-tsz"
	"github.com/influxdb/influxdb/tsdb"
)

type Value interface {
	Time() time.Time
	UnixNano() int64
	Value() interface{}
	Size() int
}

func NewValue(t time.Time, value interface{}) Value {
	switch v := value.(type) {
	// case int64:
	// 	return &Int64Value{time: t, value: v}
	case float64:
		return &FloatValue{time: t, value: v}
		// case bool:
		// 	return &BoolValue{time: t, value: v}
		// case string:
		// 	return &StringValue{time: t, value: v}
	}
	return &EmptyValue{}
}

type EmptyValue struct {
}

func (e *EmptyValue) UnixNano() int64    { return tsdb.EOF }
func (e *EmptyValue) Time() time.Time    { return time.Unix(0, tsdb.EOF) }
func (e *EmptyValue) Value() interface{} { return nil }
func (e *EmptyValue) Size() int          { return 0 }

// Values represented a time ascending sorted collection of Value types.
// the underlying type should be the same across all values, but the interface
// makes the code cleaner.
type Values []Value

func (v Values) MinTime() int64 {
	return v[0].Time().UnixNano()
}

func (v Values) MaxTime() int64 {
	return v[len(v)-1].Time().UnixNano()
}

func (v Values) Encode(buf []byte) []byte {
	switch v[0].(type) {
	case *FloatValue:
		a := make([]*FloatValue, len(v))
		for i, vv := range v {
			a[i] = vv.(*FloatValue)
		}
		return EncodeFloatBlock(buf, a)

		// TODO: add support for other types
	}

	return nil
}

func (v Values) DecodeSameTypeBlock(block []byte) Values {
	switch v[0].(type) {
	case *FloatValue:
		a, _ := DecodeFloatBlock(block)
		return a

		// TODO: add support for other types
	}
	return nil
}

// DecodeBlock takes a byte array and will decode into values of the appropriate type
// based on the block
func DecodeBlock(block []byte) (Values, error) {
	// TODO: add support for other block types
	return DecodeFloatBlock(block)
}

// Deduplicate returns a new Values slice with any values
// that have the same  timestamp removed. The Value that appears
// last in the slice is the one that is kept. The returned slice is in ascending order
func (v Values) Deduplicate() Values {
	m := make(map[int64]Value)
	for _, val := range v {
		m[val.UnixNano()] = val
	}

	a := make([]Value, 0, len(m))
	for _, val := range m {
		a = append(a, val)
	}
	sort.Sort(Values(a))

	return a
}

// Sort methods
func (a Values) Len() int           { return len(a) }
func (a Values) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Values) Less(i, j int) bool { return a[i].Time().UnixNano() < a[j].Time().UnixNano() }

type FloatValue struct {
	time  time.Time
	value float64
}

func (f *FloatValue) Time() time.Time {
	return f.time
}

func (f *FloatValue) UnixNano() int64 {
	return f.time.UnixNano()
}

func (f *FloatValue) Value() interface{} {
	return f.value
}

func (f *FloatValue) Size() int {
	return 16
}

// TODO: make this work with nanosecond timestamps
func EncodeFloatBlock(buf []byte, values []*FloatValue) []byte {
	s := tsz.New(uint32(values[0].Time().Unix()))
	for _, v := range values {
		s.Push(uint32(v.Time().Unix()), v.value)
	}
	s.Finish()
	return append(u64tob(uint64(values[0].Time().UnixNano())), s.Bytes()...)
}

func DecodeFloatBlock(block []byte) ([]Value, error) {
	iter, _ := tsz.NewIterator(block[8:])
	a := make([]Value, 0)
	for iter.Next() {
		t, f := iter.Values()
		a = append(a, &FloatValue{time.Unix(int64(t), 0), f})
	}
	return a, nil
}

type BoolValue struct {
	time  time.Time
	value bool
}

func EncodeBoolBlock(buf []byte, values []BoolValue) []byte {
	return nil
}

func DecodeBoolBlock(block []byte) ([]BoolValue, error) {
	return nil, nil
}

type Int64Value struct {
	time  time.Time
	value int64
}

func EncodeInt64Block(buf []byte, values []Int64Value) []byte {
	return nil
}

func DecodeInt64Block(block []byte) ([]Int64Value, error) {
	return nil, nil
}

type StringValue struct {
	time  time.Time
	value string
}

func EncodeStringBlock(buf []byte, values []StringValue) []byte {
	return nil
}
