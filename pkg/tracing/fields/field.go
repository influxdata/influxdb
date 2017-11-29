package fields

import (
	"fmt"
	"math"
	"time"
)

type fieldType int

const (
	stringType fieldType = iota
	boolType
	int64Type
	uint64Type
	durationType
	float64Type
)

// Field instances are constructed via Bool, String, and so on.
//
// "heavily influenced by" (i.e., partially stolen from)
// https://github.com/opentracing/opentracing-go/log
type Field struct {
	key        string
	fieldType  fieldType
	numericVal int64
	stringVal  string
}

// String adds a string-valued key:value pair to a Span.LogFields() record
func String(key, val string) Field {
	return Field{
		key:       key,
		fieldType: stringType,
		stringVal: val,
	}
}

// Bool adds a bool-valued key:value pair to a Span.LogFields() record
func Bool(key string, val bool) Field {
	var numericVal int64
	if val {
		numericVal = 1
	}
	return Field{
		key:        key,
		fieldType:  boolType,
		numericVal: numericVal,
	}
}

/// Int64 adds an int64-valued key:value pair to a Span.LogFields() record
func Int64(key string, val int64) Field {
	return Field{
		key:        key,
		fieldType:  int64Type,
		numericVal: val,
	}
}

// Uint64 adds a uint64-valued key:value pair to a Span.LogFields() record
func Uint64(key string, val uint64) Field {
	return Field{
		key:        key,
		fieldType:  uint64Type,
		numericVal: int64(val),
	}
}

// Uint64 adds a uint64-valued key:value pair to a Span.LogFields() record
func Duration(key string, val time.Duration) Field {
	return Field{
		key:        key,
		fieldType:  durationType,
		numericVal: int64(val),
	}
}

// Float64 adds a float64-valued key:value pair to a Span.LogFields() record
func Float64(key string, val float64) Field {
	return Field{
		key:        key,
		fieldType:  float64Type,
		numericVal: int64(math.Float64bits(val)),
	}
}

// Key returns the field's key.
func (lf Field) Key() string {
	return lf.key
}

// Value returns the field's value as interface{}.
func (lf Field) Value() interface{} {
	switch lf.fieldType {
	case stringType:
		return lf.stringVal
	case boolType:
		return lf.numericVal != 0
	case int64Type:
		return int64(lf.numericVal)
	case uint64Type:
		return uint64(lf.numericVal)
	case durationType:
		return time.Duration(lf.numericVal)
	case float64Type:
		return math.Float64frombits(uint64(lf.numericVal))
	default:
		return nil
	}
}

// String returns a string representation of the key and value.
func (lf Field) String() string {
	return fmt.Sprint(lf.key, ": ", lf.Value())
}
