package tsdb

import (
	"fmt"
	"math"
	"reflect"
	"time"
)

// maxTime is used as the maximum time value when computing an unbounded range.
var maxTime = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)

// Value represents a generic value type.
type Value interface{}

// valueOf returns the underlying value for a Value object.
func valueOf(v Value) interface{} {
	switch v := v.(type) {
	case *FloatValue:
		return v.Value
	default:
		panic(fmt.Sprintf("invalid value type: %T", v))
	}
}

// valueTime returns the timestamp for a Value object.
func valueTime(v Value) time.Time {
	switch v := v.(type) {
	case *FloatValue:
		return v.Time
	default:
		panic(fmt.Sprintf("invalid value type: %T", v))
	}
}

// valueNameTags returns the name & tagset for a Value object.
func valueNameTags(v Value) (name string, tags map[string]string) {
	switch v := v.(type) {
	case *FloatValue:
		return v.Name, v.Tags
	default:
		panic(fmt.Sprintf("invalid value type: %T", v))
	}
}

// Values represents a list of generic values.
type Values []Value

// Equals returns true if a is equal to other.
// This function differs from reflect.DeepEqual() in how it has NaN equality.
func (a Values) Equals(other Values) bool {
	if len(a) != len(other) {
		return false
	}
	for i := range a {
		// Special handling for float values since NaN != NaN.
		// https://github.com/golang/go/issues/12025
		if x, ok := a[i].(*FloatValue); ok {
			if y, ok := other[i].(*FloatValue); ok {
				if !x.Time.Equal(y.Time) {
					return false
				} else if x.Value != y.Value && !(math.IsNaN(x.Value) && math.IsNaN(y.Value)) {
					return false
				} else if !reflect.DeepEqual(x.Tags, y.Tags) {
					return false
				}
			} else {
				return false
			}
		} else {
			if !reflect.DeepEqual(a[i], other[i]) {
				return false
			}
		}
	}

	return true
}

// FloatValue represents a floating point typed value.
type FloatValue struct {
	Name  string
	Time  time.Time
	Value float64
	Tags  map[string]string
}
