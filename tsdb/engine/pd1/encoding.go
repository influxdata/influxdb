package pd1

import (
	"time"

	"github.com/dgryski/go-tsz"
)

type FloatValue struct {
	Time  time.Time
	Value float64
}

type FloatValues []FloatValue

func (a FloatValues) Len() int           { return len(a) }
func (a FloatValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a FloatValues) Less(i, j int) bool { return a[i].Time.UnixNano() < a[j].Time.UnixNano() }

// TODO: make this work with nanosecond timestamps
func EncodeFloatBlock(buf []byte, values []FloatValue) []byte {
	s := tsz.New(uint32(values[0].Time.Unix()))
	for _, v := range values {
		s.Push(uint32(v.Time.Unix()), v.Value)
	}
	s.Finish()
	return s.Bytes()
}

func DecodeFloatBlock(block []byte) ([]FloatValue, error) {
	iter, _ := tsz.NewIterator(block)
	a := make([]FloatValue, 0)
	for iter.Next() {
		t, f := iter.Values()
		a = append(a, FloatValue{time.Unix(int64(t), 0), f})
	}
	return a, nil
}

type BoolValue struct {
	Time  time.Time
	Value bool
}

func EncodeBoolBlock(buf []byte, values []BoolValue) []byte {
	return nil
}

func DecodeBoolBlock(block []byte) ([]BoolValue, error) {
	return nil, nil
}

type Int64Value struct {
	Time  time.Time
	Value int64
}

func EncodeInt64Block(buf []byte, values []Int64Value) []byte {
	return nil
}

func DecodeInt64Block(block []byte) ([]Int64Value, error) {
	return nil, nil
}

type StringValue struct {
	Time  time.Time
	Value string
}

func EncodeStringBlock(buf []byte, values []StringValue) []byte {
	return nil
}
