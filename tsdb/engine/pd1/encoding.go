package pd1

import (
	"sort"
	"time"

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

func EncodeFloatBlock(buf []byte, values []*FloatValue) []byte {
	if len(values) == 0 {
		return []byte{}
	}

	// A float block is encoded using different compression strategies
	// for timestamps and values.

	// Encode values using Gorilla float compression
	venc := NewFloatEncoder()

	// Encode timestamps using an adaptive encoder that uses delta-encoding,
	// frame-or-reference and run length encoding.
	tsenc := NewTimeEncoder()

	for _, v := range values {
		tsenc.Write(v.Time())
		venc.Push(v.value)
	}
	venc.Finish()

	// Encoded timestamp values
	tb, err := tsenc.Bytes()
	if err != nil {
		panic(err.Error())
	}
	// Encoded float values
	vb := venc.Bytes()

	// Preprend the first timestamp of the block in the first 8 bytes
	return append(u64tob(uint64(values[0].Time().UnixNano())),
		packBlock(tb, vb)...)
}

func DecodeFloatBlock(block []byte) ([]Value, error) {
	// The first 8 bytes is the minimum timestamp of the block
	tb, vb := unpackBlock(block[8:])

	// Setup our timestamp and value decoders
	dec := NewTimeDecoder(tb)
	iter, err := NewFloatDecoder(vb)
	if err != nil {
		return nil, err
	}

	// Decode both a timestamp and value
	var a []Value
	for dec.Next() && iter.Next() {
		ts := dec.Read()
		v := iter.Values()
		a = append(a, &FloatValue{ts, v})
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

func packBlock(ts []byte, values []byte) []byte {
	// We encode the length of the timestamp block using a variable byte encoding.
	// This allows small byte slices to take up 1 byte while larger ones use 2 or more.
	b := make([]byte, 10)
	i := binary.PutUvarint(b, uint64(len(ts)))

	// block is <len timestamp bytes>, <ts bytes>, <value bytes>
	block := append(b[:i], ts...)

	// We don't encode the value length because we know it's the rest of the block after
	// the timestamp block.
	return append(block, values...)
}

func unpackBlock(buf []byte) (ts, values []byte) {
	// Unpack the timestamp block length
	tsLen, i := binary.Uvarint(buf)

	// Unpack the timestamp bytes
	ts = buf[int(i) : int(i)+int(tsLen)]

	// Unpack the value bytes
	values = buf[int(i)+int(tsLen):]
	return
}
