package pd1

import ()

type FloatValue struct {
	Time  int64
	Value float64
}

func EncodeFloatBlock(buf []byte, values []FloatValue) []byte {
	return nil
}

func DecodeFloatBlock(block []byte) ([]FloatValue, error) {
	return nil, nil
}

type BoolValue struct {
	Time  int64
	Value bool
}

func EncodeBoolBlock(buf []byte, values []BoolValue) []byte {
	return nil
}

func DecodeBoolBlock(block []byte) ([]BoolValue, error) {
	return nil, nil
}

type Int64Value struct {
	Time  int64
	Value int64
}

func EncodeInt64Block(buf []byte, values []Int64Value) []byte {
	return nil
}

func DecodeInt64Block(block []byte) ([]Int64Value, error) {
	return nil, nil
}

type StringValue struct {
	Time  int64
	Value string
}

func EncodeStringBlock(values []StringValue) []byte {
	return nil
}
