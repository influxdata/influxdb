package tsm1

import "fmt"

type BlockType byte

const (
	// BlockFloat64 designates a block of float64 values.
	BlockFloat64 = BlockType(0)

	// BlockInteger designates a block of int64 values.
	BlockInteger = BlockType(1)

	// BlockBoolean designates a block of boolean values.
	BlockBoolean = BlockType(2)

	// BlockString designates a block of string values.
	BlockString = BlockType(3)

	// BlockUnsigned designates a block of uint64 values.
	BlockUnsigned = BlockType(4)

	// BlockUndefined represents an undefined block type value.
	BlockUndefined = BlockUnsigned + 1
)

func BlockTypeForType(v interface{}) (BlockType, error) {
	switch v.(type) {
	case int64:
		return BlockInteger, nil
	case uint64:
		return BlockUnsigned, nil
	case float64:
		return BlockFloat64, nil
	case bool:
		return BlockBoolean, nil
	case string:
		return BlockString, nil
	default:
		return BlockUndefined, fmt.Errorf("unknown type: %T", v)
	}
}
