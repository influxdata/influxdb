package cursors_test

import (
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

func makeBooleanArray(v ...interface{}) *cursors.BooleanArray {
	if len(v)&1 == 1 {
		panic("invalid array length")
	}
	a := cursors.NewBooleanArrayLen(len(v) / 2)
	for i := 0; i < len(v); i += 2 {
		a.Timestamps[i/2] = int64(v[i].(int))
		a.Values[i/2] = v[i+1].(bool)
	}
	return a
}

func makeFloatArray(v ...interface{}) *cursors.FloatArray {
	if len(v)&1 == 1 {
		panic("invalid array length")
	}
	a := cursors.NewFloatArrayLen(len(v) / 2)
	for i := 0; i < len(v); i += 2 {
		a.Timestamps[i/2] = int64(v[i].(int))
		a.Values[i/2] = v[i+1].(float64)
	}
	return a
}

func makeIntegerArray(v ...interface{}) *cursors.IntegerArray {
	if len(v)&1 == 1 {
		panic("invalid array length")
	}
	a := cursors.NewIntegerArrayLen(len(v) / 2)
	for i := 0; i < len(v); i += 2 {
		a.Timestamps[i/2] = int64(v[i].(int))
		a.Values[i/2] = int64(v[i+1].(int))
	}
	return a
}

func makeUnsignedArray(v ...interface{}) *cursors.UnsignedArray {
	if len(v)&1 == 1 {
		panic("invalid array length")
	}
	a := cursors.NewUnsignedArrayLen(len(v) / 2)
	for i := 0; i < len(v); i += 2 {
		a.Timestamps[i/2] = int64(v[i].(int))
		a.Values[i/2] = uint64(v[i+1].(int))
	}
	return a
}

func makeStringArray(v ...interface{}) *cursors.StringArray {
	if len(v)&1 == 1 {
		panic("invalid array length")
	}
	a := cursors.NewStringArrayLen(len(v) / 2)
	for i := 0; i < len(v); i += 2 {
		a.Timestamps[i/2] = int64(v[i].(int))
		a.Values[i/2] = strconv.Itoa(v[i+1].(int))
	}
	return a
}

func TestBooleanArray_Merge(t *testing.T) {
	tests := []struct {
		name      string
		a, b, exp *cursors.BooleanArray
	}{
		{
			name: "empty a",

			a:   makeBooleanArray(),
			b:   makeBooleanArray(1, true, 2, true),
			exp: makeBooleanArray(1, true, 2, true),
		},
		{
			name: "empty b",

			a:   makeBooleanArray(1, true, 2, true),
			b:   makeBooleanArray(),
			exp: makeBooleanArray(1, true, 2, true),
		},
		{
			name: "b replaces a",

			a: makeBooleanArray(1, true),
			b: makeBooleanArray(
				0, false,
				1, false, // overwrites a
				2, false,
				3, false,
				4, false,
			),
			exp: makeBooleanArray(0, false, 1, false, 2, false, 3, false, 4, false),
		},
		{
			name: "b replaces partial a",

			a: makeBooleanArray(1, true, 2, true, 3, true, 4, true),
			b: makeBooleanArray(
				1, false, // overwrites a
				2, false, // overwrites a
			),
			exp: makeBooleanArray(
				1, false, // overwrites a
				2, false, // overwrites a
				3, true,
				4, true,
			),
		},
		{
			name: "b replaces all a",

			a:   makeBooleanArray(1, true, 2, true, 3, true, 4, true),
			b:   makeBooleanArray(1, false, 2, false, 3, false, 4, false),
			exp: makeBooleanArray(1, false, 2, false, 3, false, 4, false),
		},
		{
			name: "b replaces a interleaved",
			a:    makeBooleanArray(0, true, 1, true, 2, true, 3, true, 4, true),
			b:    makeBooleanArray(0, false, 2, false, 4, false),
			exp:  makeBooleanArray(0, false, 1, true, 2, false, 3, true, 4, false),
		},
		{
			name: "b merges a interleaved",
			a:    makeBooleanArray(0, true, 2, true, 4, true),
			b:    makeBooleanArray(1, false, 3, false, 5, false),
			exp:  makeBooleanArray(0, true, 1, false, 2, true, 3, false, 4, true, 5, false),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.a.Merge(test.b)
			if !cmp.Equal(test.a, test.exp) {
				t.Fatalf("unexpected values -got/+exp\n%s", cmp.Diff(test.a, test.exp))
			}
		})
	}
}

func TestFloatArray_Merge(t *testing.T) {
	tests := []struct {
		name      string
		a, b, exp *cursors.FloatArray
	}{
		{
			name: "empty a",

			a:   makeFloatArray(),
			b:   makeFloatArray(1, 1.1, 2, 2.1),
			exp: makeFloatArray(1, 1.1, 2, 2.1),
		},
		{
			name: "empty b",

			a:   makeFloatArray(1, 1.0, 2, 2.0),
			b:   makeFloatArray(),
			exp: makeFloatArray(1, 1.0, 2, 2.0),
		},
		{
			name: "b replaces a",

			a: makeFloatArray(1, 1.0),
			b: makeFloatArray(
				0, 0.1,
				1, 1.1, // overwrites a
				2, 2.1,
				3, 3.1,
				4, 4.1,
			),
			exp: makeFloatArray(0, 0.1, 1, 1.1, 2, 2.1, 3, 3.1, 4, 4.1),
		},
		{
			name: "b replaces partial a",

			a: makeFloatArray(1, 1.0, 2, 2.0, 3, 3.0, 4, 4.0),
			b: makeFloatArray(
				1, 1.1, // overwrites a
				2, 2.1, // overwrites a
			),
			exp: makeFloatArray(
				1, 1.1, // overwrites a
				2, 2.1, // overwrites a
				3, 3.0,
				4, 4.0,
			),
		},
		{
			name: "b replaces all a",

			a:   makeFloatArray(1, 1.0, 2, 2.0, 3, 3.0, 4, 4.0),
			b:   makeFloatArray(1, 1.1, 2, 2.1, 3, 3.1, 4, 4.1),
			exp: makeFloatArray(1, 1.1, 2, 2.1, 3, 3.1, 4, 4.1),
		},
		{
			name: "b replaces a interleaved",
			a:    makeFloatArray(0, 0.0, 1, 1.0, 2, 2.0, 3, 3.0, 4, 4.0),
			b:    makeFloatArray(0, 0.1, 2, 2.1, 4, 4.1),
			exp:  makeFloatArray(0, 0.1, 1, 1.0, 2, 2.1, 3, 3.0, 4, 4.1),
		},
		{
			name: "b merges a interleaved",
			a:    makeFloatArray(0, 0.0, 2, 2.0, 4, 4.0),
			b:    makeFloatArray(1, 1.1, 3, 3.1, 5, 5.1),
			exp:  makeFloatArray(0, 0.0, 1, 1.1, 2, 2.0, 3, 3.1, 4, 4.0, 5, 5.1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.a.Merge(test.b)
			if !cmp.Equal(test.a, test.exp) {
				t.Fatalf("unexpected values -got/+exp\n%s", cmp.Diff(test.a, test.exp))
			}
		})
	}
}

func TestIntegerArray_Merge(t *testing.T) {
	tests := []struct {
		name      string
		a, b, exp *cursors.IntegerArray
	}{
		{
			name: "empty a",

			a:   makeIntegerArray(),
			b:   makeIntegerArray(1, 11, 2, 21),
			exp: makeIntegerArray(1, 11, 2, 21),
		},
		{
			name: "empty b",

			a:   makeIntegerArray(1, 10, 2, 20),
			b:   makeIntegerArray(),
			exp: makeIntegerArray(1, 10, 2, 20),
		},
		{
			name: "b replaces a",

			a: makeIntegerArray(1, 10),
			b: makeIntegerArray(
				0, 1,
				1, 11, // overwrites a
				2, 21,
				3, 31,
				4, 41,
			),
			exp: makeIntegerArray(0, 1, 1, 11, 2, 21, 3, 31, 4, 41),
		},
		{
			name: "b replaces partial a",

			a: makeIntegerArray(1, 10, 2, 20, 3, 30, 4, 40),
			b: makeIntegerArray(
				1, 11, // overwrites a
				2, 21, // overwrites a
			),
			exp: makeIntegerArray(
				1, 11, // overwrites a
				2, 21, // overwrites a
				3, 30,
				4, 40,
			),
		},
		{
			name: "b replaces all a",

			a:   makeIntegerArray(1, 10, 2, 20, 3, 30, 4, 40),
			b:   makeIntegerArray(1, 11, 2, 21, 3, 31, 4, 41),
			exp: makeIntegerArray(1, 11, 2, 21, 3, 31, 4, 41),
		},
		{
			name: "b replaces a interleaved",
			a:    makeIntegerArray(0, 0, 1, 10, 2, 20, 3, 30, 4, 40),
			b:    makeIntegerArray(0, 1, 2, 21, 4, 41),
			exp:  makeIntegerArray(0, 1, 1, 10, 2, 21, 3, 30, 4, 41),
		},
		{
			name: "b merges a interleaved",
			a:    makeIntegerArray(0, 00, 2, 20, 4, 40),
			b:    makeIntegerArray(1, 11, 3, 31, 5, 51),
			exp:  makeIntegerArray(0, 00, 1, 11, 2, 20, 3, 31, 4, 40, 5, 51),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.a.Merge(test.b)
			if !cmp.Equal(test.a, test.exp) {
				t.Fatalf("unexpected values -got/+exp\n%s", cmp.Diff(test.a, test.exp))
			}
		})
	}
}

func TestUnsignedArray_Merge(t *testing.T) {
	tests := []struct {
		name      string
		a, b, exp *cursors.UnsignedArray
	}{
		{
			name: "empty a",

			a:   makeUnsignedArray(),
			b:   makeUnsignedArray(1, 11, 2, 21),
			exp: makeUnsignedArray(1, 11, 2, 21),
		},
		{
			name: "empty b",

			a:   makeUnsignedArray(1, 10, 2, 20),
			b:   makeUnsignedArray(),
			exp: makeUnsignedArray(1, 10, 2, 20),
		},
		{
			name: "b replaces a",

			a: makeUnsignedArray(1, 10),
			b: makeUnsignedArray(
				0, 1,
				1, 11, // overwrites a
				2, 21,
				3, 31,
				4, 41,
			),
			exp: makeUnsignedArray(0, 1, 1, 11, 2, 21, 3, 31, 4, 41),
		},
		{
			name: "b replaces partial a",

			a: makeUnsignedArray(1, 10, 2, 20, 3, 30, 4, 40),
			b: makeUnsignedArray(
				1, 11, // overwrites a
				2, 21, // overwrites a
			),
			exp: makeUnsignedArray(
				1, 11, // overwrites a
				2, 21, // overwrites a
				3, 30,
				4, 40,
			),
		},
		{
			name: "b replaces all a",

			a:   makeUnsignedArray(1, 10, 2, 20, 3, 30, 4, 40),
			b:   makeUnsignedArray(1, 11, 2, 21, 3, 31, 4, 41),
			exp: makeUnsignedArray(1, 11, 2, 21, 3, 31, 4, 41),
		},
		{
			name: "b replaces a interleaved",
			a:    makeUnsignedArray(0, 0, 1, 10, 2, 20, 3, 30, 4, 40),
			b:    makeUnsignedArray(0, 1, 2, 21, 4, 41),
			exp:  makeUnsignedArray(0, 1, 1, 10, 2, 21, 3, 30, 4, 41),
		},
		{
			name: "b merges a interleaved",
			a:    makeUnsignedArray(0, 00, 2, 20, 4, 40),
			b:    makeUnsignedArray(1, 11, 3, 31, 5, 51),
			exp:  makeUnsignedArray(0, 00, 1, 11, 2, 20, 3, 31, 4, 40, 5, 51),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.a.Merge(test.b)
			if !cmp.Equal(test.a, test.exp) {
				t.Fatalf("unexpected values -got/+exp\n%s", cmp.Diff(test.a, test.exp))
			}
		})
	}
}

func TestStringArray_Merge(t *testing.T) {
	tests := []struct {
		name      string
		a, b, exp *cursors.StringArray
	}{
		{
			name: "empty a",

			a:   makeStringArray(),
			b:   makeStringArray(1, 11, 2, 21),
			exp: makeStringArray(1, 11, 2, 21),
		},
		{
			name: "empty b",

			a:   makeStringArray(1, 10, 2, 20),
			b:   makeStringArray(),
			exp: makeStringArray(1, 10, 2, 20),
		},
		{
			name: "b replaces a",

			a: makeStringArray(1, 10),
			b: makeStringArray(
				0, 1,
				1, 11, // overwrites a
				2, 21,
				3, 31,
				4, 41,
			),
			exp: makeStringArray(0, 1, 1, 11, 2, 21, 3, 31, 4, 41),
		},
		{
			name: "b replaces partial a",

			a: makeStringArray(1, 10, 2, 20, 3, 30, 4, 40),
			b: makeStringArray(
				1, 11, // overwrites a
				2, 21, // overwrites a
			),
			exp: makeStringArray(
				1, 11, // overwrites a
				2, 21, // overwrites a
				3, 30,
				4, 40,
			),
		},
		{
			name: "b replaces all a",

			a:   makeStringArray(1, 10, 2, 20, 3, 30, 4, 40),
			b:   makeStringArray(1, 11, 2, 21, 3, 31, 4, 41),
			exp: makeStringArray(1, 11, 2, 21, 3, 31, 4, 41),
		},
		{
			name: "b replaces a interleaved",
			a:    makeStringArray(0, 0, 1, 10, 2, 20, 3, 30, 4, 40),
			b:    makeStringArray(0, 1, 2, 21, 4, 41),
			exp:  makeStringArray(0, 1, 1, 10, 2, 21, 3, 30, 4, 41),
		},
		{
			name: "b merges a interleaved",
			a:    makeStringArray(0, 00, 2, 20, 4, 40),
			b:    makeStringArray(1, 11, 3, 31, 5, 51),
			exp:  makeStringArray(0, 00, 1, 11, 2, 20, 3, 31, 4, 40, 5, 51),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.a.Merge(test.b)
			if !cmp.Equal(test.a, test.exp) {
				t.Fatalf("unexpected values -got/+exp\n%s", cmp.Diff(test.a, test.exp))
			}
		})
	}
}
