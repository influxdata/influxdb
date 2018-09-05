package tsm1

import (
	"fmt"

	"github.com/influxdata/influxdb/tsdb"
)

// DecodeBooleanArrayBlock decodes the boolean block from the byte slice
// and writes the values to a.
func DecodeBooleanArrayBlock(block []byte, a *tsdb.BooleanArray) error {
	blockType := block[0]
	if blockType != BlockBoolean {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockBoolean, blockType)
	}

	tb, vb, err := unpackBlock(block[1:])
	if err != nil {
		return err
	}

	a.Timestamps, err = TimeArrayDecodeAll(tb, a.Timestamps)
	if err != nil {
		return err
	}
	a.Values, err = BooleanArrayDecodeAll(vb, a.Values)
	return err
}

// DecodeFloatArrayBlock decodes the float block from the byte slice
// and writes the values to a.
func DecodeFloatArrayBlock(block []byte, a *tsdb.FloatArray) error {
	blockType := block[0]
	if blockType != BlockFloat64 {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockFloat64, blockType)
	}

	tb, vb, err := unpackBlock(block[1:])
	if err != nil {
		return err
	}

	a.Timestamps, err = TimeArrayDecodeAll(tb, a.Timestamps)
	if err != nil {
		return err
	}
	a.Values, err = FloatArrayDecodeAll(vb, a.Values)
	return err
}

// DecodeIntegerArrayBlock decodes the integer block from the byte slice
// and writes the values to a.
func DecodeIntegerArrayBlock(block []byte, a *tsdb.IntegerArray) error {
	blockType := block[0]
	if blockType != BlockInteger {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockInteger, blockType)
	}

	tb, vb, err := unpackBlock(block[1:])
	if err != nil {
		return err
	}

	a.Timestamps, err = TimeArrayDecodeAll(tb, a.Timestamps)
	if err != nil {
		return err
	}
	a.Values, err = IntegerArrayDecodeAll(vb, a.Values)
	return err
}

// DecodeUnsignedArrayBlock decodes the unsigned integer block from the byte slice
// and writes the values to a.
func DecodeUnsignedArrayBlock(block []byte, a *tsdb.UnsignedArray) error {
	blockType := block[0]
	if blockType != BlockUnsigned {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockUnsigned, blockType)
	}

	tb, vb, err := unpackBlock(block[1:])
	if err != nil {
		return err
	}

	a.Timestamps, err = TimeArrayDecodeAll(tb, a.Timestamps)
	if err != nil {
		return err
	}
	a.Values, err = UnsignedArrayDecodeAll(vb, a.Values)
	return err
}

// DecodeStringArrayBlock decodes the string block from the byte slice
// and writes the values to a.
func DecodeStringArrayBlock(block []byte, a *tsdb.StringArray) error {
	blockType := block[0]
	if blockType != BlockString {
		return fmt.Errorf("invalid block type: exp %d, got %d", BlockString, blockType)
	}

	tb, vb, err := unpackBlock(block[1:])
	if err != nil {
		return err
	}

	a.Timestamps, err = TimeArrayDecodeAll(tb, a.Timestamps)
	if err != nil {
		return err
	}
	a.Values, err = StringArrayDecodeAll(vb, a.Values)
	return err
}
