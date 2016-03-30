package tsm1

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/tsdb"
)

const (
	// BlockFloat64 designates a block encodes float64 values
	BlockFloat64 = byte(0)

	// BlockInteger designates a block encodes int64 values
	BlockInteger = byte(1)

	// BlockBoolean designates a block encodes boolean values
	BlockBoolean = byte(2)

	// BlockString designates a block encodes string values
	BlockString = byte(3)

	// encodedBlockHeaderSize is the size of the header for an encoded block.  There is one
	// byte encoding the type of the block.
	encodedBlockHeaderSize = 1
)

type Value interface {
	UnixNano() int64
	Value() interface{}
	Size() int
	String() string

	internalOnly()
}

func NewValue(t int64, value interface{}) Value {
	switch v := value.(type) {
	case int64:
		return &IntegerValue{unixnano: t, value: v}
	case float64:
		return &FloatValue{unixnano: t, value: v}
	case bool:
		return &BooleanValue{unixnano: t, value: v}
	case string:
		return &StringValue{unixnano: t, value: v}
	}
	return &EmptyValue{}
}

type EmptyValue struct {
}

func (e *EmptyValue) UnixNano() int64    { return tsdb.EOF }
func (e *EmptyValue) Value() interface{} { return nil }
func (e *EmptyValue) Size() int          { return 0 }
func (e *EmptyValue) String() string     { return "" }

func (_ *EmptyValue) internalOnly()   {}
func (_ *StringValue) internalOnly()  {}
func (_ *IntegerValue) internalOnly() {}
func (_ *BooleanValue) internalOnly() {}
func (_ *FloatValue) internalOnly()   {}

// Values represented a time ascending sorted collection of Value types.
// the underlying type should be the same across all values, but the interface
// makes the code cleaner.
type Values []Value

func (a Values) MinTime() int64 {
	return a[0].UnixNano()
}

func (a Values) MaxTime() int64 {
	return a[len(a)-1].UnixNano()
}

func (a Values) Size() int {
	sz := 0
	for _, v := range a {
		sz += v.Size()
	}
	return sz
}

// Encode converts the values to a byte slice.  If there are no values,
// this function panics.
func (a Values) Encode(buf []byte) ([]byte, error) {
	if len(a) == 0 {
		panic("unable to encode block type")
	}

	switch a[0].(type) {
	case *FloatValue:
		return encodeFloatBlock(buf, a)
	case *IntegerValue:
		return encodeIntegerBlock(buf, a)
	case *BooleanValue:
		return encodeBooleanBlock(buf, a)
	case *StringValue:
		return encodeStringBlock(buf, a)
	}

	return nil, fmt.Errorf("unsupported value type %T", a[0])
}

// InfluxQLType returns the influxql.DataType the values map to.
func (a Values) InfluxQLType() (influxql.DataType, error) {
	if len(a) == 0 {
		return influxql.Unknown, fmt.Errorf("no values to infer type")
	}

	switch a[0].(type) {
	case *FloatValue:
		return influxql.Float, nil
	case *IntegerValue:
		return influxql.Integer, nil
	case *BooleanValue:
		return influxql.Boolean, nil
	case *StringValue:
		return influxql.String, nil
	}

	return influxql.Unknown, fmt.Errorf("unsupported value type %T", a[0])
}

// BlockType returns the type of value encoded in a block or an error
// if the block type is unknown.
func BlockType(block []byte) (byte, error) {
	blockType := block[0]
	switch blockType {
	case BlockFloat64, BlockInteger, BlockBoolean, BlockString:
		return blockType, nil
	default:
		return 0, fmt.Errorf("unknown block type: %d", blockType)
	}
}

func BlockCount(block []byte) int {
	if len(block) <= encodedBlockHeaderSize {
		panic(fmt.Sprintf("count of short block: got %v, exp %v", len(block), encodedBlockHeaderSize))
	}
	// first byte is the block type
	tb, _ := unpackBlock(block[1:])
	return CountTimestamps(tb)
}

// DecodeBlock takes a byte array and will decode into values of the appropriate type
// based on the block.
func DecodeBlock(block []byte, vals []Value) ([]Value, error) {
	if len(block) <= encodedBlockHeaderSize {
		panic(fmt.Sprintf("decode of short block: got %v, exp %v", len(block), encodedBlockHeaderSize))
	}

	blockType, err := BlockType(block)
	if err != nil {
		return nil, err
	}

	switch blockType {
	case BlockFloat64:
		decoded, err := DecodeFloatBlock(block, nil)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = &decoded[i]
		}
		return vals[:len(decoded)], err
	case BlockInteger:
		decoded, err := DecodeIntegerBlock(block, nil)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = &decoded[i]
		}
		return vals[:len(decoded)], err

	case BlockBoolean:
		decoded, err := DecodeBooleanBlock(block, nil)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = &decoded[i]
		}
		return vals[:len(decoded)], err

	case BlockString:
		decoded, err := DecodeStringBlock(block, nil)
		if len(vals) < len(decoded) {
			vals = make([]Value, len(decoded))
		}
		for i := range decoded {
			vals[i] = &decoded[i]
		}
		return vals[:len(decoded)], err

	default:
		panic(fmt.Sprintf("unknown block type: %d", blockType))
	}
}

// Deduplicate returns a new Values slice with any values that have the same timestamp removed.
// The Value that appears last in the slice is the one that is kept.
func (a Values) Deduplicate() Values {
	m := make(map[int64]Value, len(a))
	for _, val := range a {
		m[val.UnixNano()] = val
	}

	other := make([]Value, 0, len(m))
	for _, val := range m {
		other = append(other, val)
	}

	sort.Sort(Values(other))
	return other
}

// Sort methods
func (a Values) Len() int           { return len(a) }
func (a Values) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Values) Less(i, j int) bool { return a[i].UnixNano() < a[j].UnixNano() }

type FloatValue struct {
	unixnano int64
	value    float64
}

func (f *FloatValue) UnixNano() int64 {
	return f.unixnano
}

func (f *FloatValue) Value() interface{} {
	return f.value
}

func (f *FloatValue) Size() int {
	return 16
}

func (f *FloatValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, f.unixnano), f.value)
}

func encodeFloatBlock(buf []byte, values []Value) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// A float block is encoded using different compression strategies
	// for timestamps and values.

	// Encode values using Gorilla float compression
	venc := NewFloatEncoder()

	// Encode timestamps using an adaptive encoder that uses delta-encoding,
	// frame-or-reference and run length encoding.
	tsenc := NewTimeEncoder()

	for _, v := range values {
		tsenc.Write(time.Unix(0, v.UnixNano()))
		venc.Push(v.(*FloatValue).value)
	}
	venc.Finish()

	// Encoded timestamp values
	tb, err := tsenc.Bytes()
	if err != nil {
		return nil, err
	}
	// Encoded float values
	vb, err := venc.Bytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes and the block
	// in the next byte, followed by the block
	block := packBlockHeader(BlockFloat64)
	block = append(block, packBlock(tb, vb)...)
	return block, nil
}

func DecodeFloatBlock(block []byte, a []FloatValue) ([]FloatValue, error) {
	// Block type is the next block, make sure we actually have a float block
	blockType := block[0]
	if blockType != BlockFloat64 {
		return nil, fmt.Errorf("invalid block type: exp %d, got %d", BlockFloat64, blockType)
	}
	block = block[1:]

	tb, vb := unpackBlock(block)

	// Setup our timestamp and value decoders
	dec := NewTimeDecoder(tb)
	iter, err := NewFloatDecoder(vb)
	if err != nil {
		return nil, err
	}

	// Decode both a timestamp and value
	i := 0
	for dec.Next() && iter.Next() {
		ts := dec.Read()
		v := iter.Values()
		if i < len(a) {
			a[i].unixnano = ts.UnixNano()
			a[i].value = v
		} else {
			a = append(a, FloatValue{ts.UnixNano(), v})
		}
		i++
	}

	// Did timestamp decoding have an error?
	if dec.Error() != nil {
		return nil, dec.Error()
	}
	// Did float decoding have an error?
	if iter.Error() != nil {
		return nil, iter.Error()
	}

	return a[:i], nil
}

// FloatValues represents a slice of float values.
type FloatValues []FloatValue

// Deduplicate returns a new slice with any values that have the same timestamp removed.
// The Value that appears last in the slice is the one that is kept.
func (a FloatValues) Deduplicate() FloatValues {
	m := make(map[int64]FloatValue)
	for _, val := range a {
		m[val.UnixNano()] = val
	}

	other := make(FloatValues, 0, len(m))
	for _, val := range m {
		other = append(other, val)
	}

	sort.Sort(other)
	return other
}

// Sort methods
func (a FloatValues) Len() int           { return len(a) }
func (a FloatValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a FloatValues) Less(i, j int) bool { return a[i].UnixNano() < a[j].UnixNano() }

type BooleanValue struct {
	unixnano int64
	value    bool
}

func (b *BooleanValue) Size() int {
	return 9
}

func (b *BooleanValue) UnixNano() int64 {
	return b.unixnano
}

func (b *BooleanValue) Value() interface{} {
	return b.value
}

func (f *BooleanValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, f.unixnano), f.Value())
}

func encodeBooleanBlock(buf []byte, values []Value) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// A boolean block is encoded using different compression strategies
	// for timestamps and values.

	// Encode values using Gorilla float compression
	venc := NewBooleanEncoder()

	// Encode timestamps using an adaptive encoder
	tsenc := NewTimeEncoder()

	for _, v := range values {
		tsenc.Write(time.Unix(0, v.UnixNano()))
		venc.Write(v.(*BooleanValue).value)
	}

	// Encoded timestamp values
	tb, err := tsenc.Bytes()
	if err != nil {
		return nil, err
	}
	// Encoded float values
	vb, err := venc.Bytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes and the block
	// in the next byte, followed by the block
	block := packBlockHeader(BlockBoolean)
	block = append(block, packBlock(tb, vb)...)
	return block, nil
}

func DecodeBooleanBlock(block []byte, a []BooleanValue) ([]BooleanValue, error) {
	// Block type is the next block, make sure we actually have a float block
	blockType := block[0]
	if blockType != BlockBoolean {
		return nil, fmt.Errorf("invalid block type: exp %d, got %d", BlockBoolean, blockType)
	}
	block = block[1:]

	tb, vb := unpackBlock(block)

	// Setup our timestamp and value decoders
	dec := NewTimeDecoder(tb)
	vdec := NewBooleanDecoder(vb)

	// Decode both a timestamp and value
	i := 0
	for dec.Next() && vdec.Next() {
		ts := dec.Read()
		v := vdec.Read()
		if i < len(a) {
			a[i].unixnano = ts.UnixNano()
			a[i].value = v
		} else {
			a = append(a, BooleanValue{ts.UnixNano(), v})
		}
		i++
	}

	// Did timestamp decoding have an error?
	if dec.Error() != nil {
		return nil, dec.Error()
	}
	// Did boolean decoding have an error?
	if vdec.Error() != nil {
		return nil, vdec.Error()
	}

	return a[:i], nil
}

// BooleanValues represents a slice of boolean values.
type BooleanValues []BooleanValue

// Deduplicate returns a new slice with any values that have the same timestamp removed.
// The Value that appears last in the slice is the one that is kept.
func (a BooleanValues) Deduplicate() BooleanValues {
	m := make(map[int64]BooleanValue)
	for _, val := range a {
		m[val.UnixNano()] = val
	}

	other := make(BooleanValues, 0, len(m))
	for _, val := range m {
		other = append(other, val)
	}

	sort.Sort(other)
	return other
}

// Sort methods
func (a BooleanValues) Len() int           { return len(a) }
func (a BooleanValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BooleanValues) Less(i, j int) bool { return a[i].UnixNano() < a[j].UnixNano() }

type IntegerValue struct {
	unixnano int64
	value    int64
}

func (v *IntegerValue) Value() interface{} {
	return v.value
}

func (v *IntegerValue) UnixNano() int64 {
	return v.unixnano
}

func (v *IntegerValue) Size() int {
	return 16
}

func (f *IntegerValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, f.unixnano), f.Value())
}

func encodeIntegerBlock(buf []byte, values []Value) ([]byte, error) {
	tsEnc := NewTimeEncoder()
	vEnc := NewIntegerEncoder()
	for _, v := range values {
		tsEnc.Write(time.Unix(0, v.UnixNano()))
		vEnc.Write(v.(*IntegerValue).value)
	}

	// Encoded timestamp values
	tb, err := tsEnc.Bytes()
	if err != nil {
		return nil, err
	}
	// Encoded int64 values
	vb, err := vEnc.Bytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes
	block := packBlockHeader(BlockInteger)
	return append(block, packBlock(tb, vb)...), nil
}

func DecodeIntegerBlock(block []byte, a []IntegerValue) ([]IntegerValue, error) {
	blockType := block[0]
	if blockType != BlockInteger {
		return nil, fmt.Errorf("invalid block type: exp %d, got %d", BlockInteger, blockType)
	}

	block = block[1:]

	// The first 8 bytes is the minimum timestamp of the block
	tb, vb := unpackBlock(block)

	// Setup our timestamp and value decoders
	tsDec := NewTimeDecoder(tb)
	vDec := NewIntegerDecoder(vb)

	// Decode both a timestamp and value
	i := 0
	for tsDec.Next() && vDec.Next() {
		ts := tsDec.Read()
		v := vDec.Read()
		if i < len(a) {
			a[i].unixnano = ts.UnixNano()
			a[i].value = v
		} else {
			a = append(a, IntegerValue{ts.UnixNano(), v})
		}
		i++
	}

	// Did timestamp decoding have an error?
	if tsDec.Error() != nil {
		return nil, tsDec.Error()
	}
	// Did int64 decoding have an error?
	if vDec.Error() != nil {
		return nil, vDec.Error()
	}

	return a[:i], nil
}

// IntegerValues represents a slice of integer values.
type IntegerValues []IntegerValue

// Deduplicate returns a new slice with any values that have the same timestamp removed.
// The Value that appears last in the slice is the one that is kept.
func (a IntegerValues) Deduplicate() IntegerValues {
	m := make(map[int64]IntegerValue)
	for _, val := range a {
		m[val.UnixNano()] = val
	}

	other := make(IntegerValues, 0, len(m))
	for _, val := range m {
		other = append(other, val)
	}

	sort.Sort(other)
	return other
}

// Sort methods
func (a IntegerValues) Len() int           { return len(a) }
func (a IntegerValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a IntegerValues) Less(i, j int) bool { return a[i].UnixNano() < a[j].UnixNano() }

type StringValue struct {
	unixnano int64
	value    string
}

func (v *StringValue) Value() interface{} {
	return v.value
}

func (v *StringValue) UnixNano() int64 {
	return v.unixnano
}

func (v *StringValue) Size() int {
	return 8 + len(v.value)
}

func (f *StringValue) String() string {
	return fmt.Sprintf("%v %v", time.Unix(0, f.unixnano), f.Value())
}

func encodeStringBlock(buf []byte, values []Value) ([]byte, error) {
	tsEnc := NewTimeEncoder()
	vEnc := NewStringEncoder()
	for _, v := range values {
		tsEnc.Write(time.Unix(0, v.UnixNano()))
		vEnc.Write(v.(*StringValue).value)
	}

	// Encoded timestamp values
	tb, err := tsEnc.Bytes()
	if err != nil {
		return nil, err
	}
	// Encoded string values
	vb, err := vEnc.Bytes()
	if err != nil {
		return nil, err
	}

	// Prepend the first timestamp of the block in the first 8 bytes
	block := packBlockHeader(BlockString)
	return append(block, packBlock(tb, vb)...), nil
}

func DecodeStringBlock(block []byte, a []StringValue) ([]StringValue, error) {
	blockType := block[0]
	if blockType != BlockString {
		return nil, fmt.Errorf("invalid block type: exp %d, got %d", BlockString, blockType)
	}

	block = block[1:]

	// The first 8 bytes is the minimum timestamp of the block
	tb, vb := unpackBlock(block)

	// Setup our timestamp and value decoders
	tsDec := NewTimeDecoder(tb)
	vDec, err := NewStringDecoder(vb)
	if err != nil {
		return nil, err
	}

	// Decode both a timestamp and value
	i := 0
	for tsDec.Next() && vDec.Next() {
		ts := tsDec.Read()
		v := vDec.Read()
		if i < len(a) {
			a[i].unixnano = ts.UnixNano()
			a[i].value = v
		} else {
			a = append(a, StringValue{ts.UnixNano(), v})
		}
		i++
	}

	// Did timestamp decoding have an error?
	if tsDec.Error() != nil {
		return nil, tsDec.Error()
	}
	// Did string decoding have an error?
	if vDec.Error() != nil {
		return nil, vDec.Error()
	}

	return a[:i], nil
}

// StringValues represents a slice of string values.
type StringValues []StringValue

// Deduplicate returns a new slice with any values that have the same timestamp removed.
// The Value that appears last in the slice is the one that is kept.
func (a StringValues) Deduplicate() StringValues {
	m := make(map[int64]StringValue)
	for _, val := range a {
		m[val.UnixNano()] = val
	}

	other := make(StringValues, 0, len(m))
	for _, val := range m {
		other = append(other, val)
	}

	sort.Sort(other)
	return other
}

// Sort methods
func (a StringValues) Len() int           { return len(a) }
func (a StringValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a StringValues) Less(i, j int) bool { return a[i].UnixNano() < a[j].UnixNano() }

func packBlockHeader(blockType byte) []byte {
	return []byte{blockType}
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

// ZigZagEncode converts a int64 to a uint64 by zig zagging negative and positive values
// across even and odd numbers.  Eg. [0,-1,1,-2] becomes [0, 1, 2, 3]
func ZigZagEncode(x int64) uint64 {
	return uint64(uint64(x<<1) ^ uint64((int64(x) >> 63)))
}

// ZigZagDecode converts a previously zigzag encoded uint64 back to a int64
func ZigZagDecode(v uint64) int64 {
	return int64((v >> 1) ^ uint64((int64(v&1)<<63)>>63))
}
