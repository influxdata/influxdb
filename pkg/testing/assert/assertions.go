package assert

import (
	"bytes"
	"fmt"
	"reflect"
)

type TestingT interface {
	Helper()
	Errorf(format string, args ...interface{})
}

// Equal asserts that the values are equal and returns
// true if the assertion was successful.
func Equal(t TestingT, got, expected interface{}, msgAndArgs ...interface{}) bool {
	if ValuesAreEqual(got, expected) {
		return true
	}

	t.Helper()
	got, expected = formatValues(got, expected)
	fail(t, fmt.Sprintf("Not Equal: got=%s, exp=%s", got, expected), msgAndArgs...)
	return false
}

// NotEqual asserts that the values are not equal and returns
// true if the assertion was successful.
func NotEqual(t TestingT, got, expected interface{}, msgAndArgs ...interface{}) bool {
	if !ValuesAreEqual(got, expected) {
		return true
	}

	t.Helper()
	_, expected = formatValues(got, expected)
	fail(t, fmt.Sprintf("Equal: should not be %s", expected), msgAndArgs...)
	return false
}

// PanicsWithValue asserts that fn panics, and that
// the recovered panic value equals the expected panic value.
//
// Returns true if the assertion was successful.
func PanicsWithValue(t TestingT, expected interface{}, fn PanicTestFunc, msgAndArgs ...interface{}) bool {
	t.Helper()
	if funcDidPanic, got := didPanic(fn); !funcDidPanic {
		return fail(t, fmt.Sprintf("func %#v should panic\n\r\tPanic value:\t%v", fn, got), msgAndArgs...)
	} else if got != expected {
		return fail(t, fmt.Sprintf("func %#v should panic with value:\t%v\n\r\tPanic value:\t%v", fn, expected, got), msgAndArgs...)
	}

	return true
}

// ValuesAreEqual determines if the values are equal.
func ValuesAreEqual(got, expected interface{}) bool {
	if got == nil || expected == nil {
		return got == expected
	}

	if exp, ok := expected.([]byte); ok {
		act, ok := got.([]byte)
		if !ok {
			return false
		} else if exp == nil || act == nil {
			return exp == nil && act == nil
		}
		return bytes.Equal(exp, act)
	}

	return reflect.DeepEqual(expected, got)

}

// ValuesAreExactlyEqual determines if the values are equal and
// their types are the same.
func ValuesAreExactlyEqual(got, expected interface{}) bool {
	if ValuesAreEqual(got, expected) {
		return true
	}

	actualType := reflect.TypeOf(got)
	if actualType == nil {
		return false
	}
	expectedValue := reflect.ValueOf(expected)
	if expectedValue.IsValid() && expectedValue.Type().ConvertibleTo(actualType) {
		// Attempt comparison after type conversion
		return reflect.DeepEqual(expectedValue.Convert(actualType).Interface(), got)
	}

	return false
}

// PanicTestFunc defines a function that is called to determine whether a panic occurs.
type PanicTestFunc func()
