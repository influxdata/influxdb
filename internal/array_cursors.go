package internal

import "github.com/influxdata/influxdb/tsdb"

var (
	_ tsdb.IntegerArrayCursor  = NewIntegerArrayCursorMock()
	_ tsdb.FloatArrayCursor    = NewFloatArrayCursorMock()
	_ tsdb.UnsignedArrayCursor = NewUnsignedArrayCursorMock()
	_ tsdb.StringArrayCursor   = NewStringArrayCursorMock()
	_ tsdb.BooleanArrayCursor  = NewBooleanArrayCursorMock()
)

// ArrayCursorMock provides a mock base implementation for batch cursors.
type ArrayCursorMock struct {
	CloseFn func()
	ErrFn   func() error
	StatsFn func() tsdb.CursorStats
}

// NewArrayCursorMock returns an initialised ArrayCursorMock, which
// returns the zero value for all methods.
func NewArrayCursorMock() *ArrayCursorMock {
	return &ArrayCursorMock{
		CloseFn: func() {},
		ErrFn:   func() error { return nil },
		StatsFn: func() tsdb.CursorStats { return tsdb.CursorStats{} },
	}
}

// Close closes the cursor.
func (c *ArrayCursorMock) Close() { c.CloseFn() }

// Err returns the latest error, if any.
func (c *ArrayCursorMock) Err() error { return c.ErrFn() }

func (c *ArrayCursorMock) Stats() tsdb.CursorStats {
	return c.StatsFn()
}

// IntegerArrayCursorMock provides a mock implementation of an IntegerArrayCursorMock.
type IntegerArrayCursorMock struct {
	*ArrayCursorMock
	NextFn func() *tsdb.IntegerArray
}

// NewIntegerArrayCursorMock returns an initialised IntegerArrayCursorMock, which
// returns the zero value for all methods.
func NewIntegerArrayCursorMock() *IntegerArrayCursorMock {
	return &IntegerArrayCursorMock{
		ArrayCursorMock: NewArrayCursorMock(),
		NextFn:          func() *tsdb.IntegerArray { return tsdb.NewIntegerArrayLen(0) },
	}
}

// Next returns the next set of keys and values.
func (c *IntegerArrayCursorMock) Next() *tsdb.IntegerArray {
	return c.NextFn()
}

// FloatArrayCursorMock provides a mock implementation of a FloatArrayCursor.
type FloatArrayCursorMock struct {
	*ArrayCursorMock
	NextFn func() *tsdb.FloatArray
}

// NewFloatArrayCursorMock returns an initialised FloatArrayCursorMock, which
// returns the zero value for all methods.
func NewFloatArrayCursorMock() *FloatArrayCursorMock {
	return &FloatArrayCursorMock{
		ArrayCursorMock: NewArrayCursorMock(),
		NextFn:          func() *tsdb.FloatArray { return tsdb.NewFloatArrayLen(0) },
	}
}

// Next returns the next set of keys and values.
func (c *FloatArrayCursorMock) Next() *tsdb.FloatArray {
	return c.NextFn()
}

// UnsignedArrayCursorMock provides a mock implementation of an UnsignedArrayCursorMock.
type UnsignedArrayCursorMock struct {
	*ArrayCursorMock
	NextFn func() *tsdb.UnsignedArray
}

// NewUnsignedArrayCursorMock returns an initialised UnsignedArrayCursorMock, which
// returns the zero value for all methods.
func NewUnsignedArrayCursorMock() *UnsignedArrayCursorMock {
	return &UnsignedArrayCursorMock{
		ArrayCursorMock: NewArrayCursorMock(),
		NextFn:          func() *tsdb.UnsignedArray { return tsdb.NewUnsignedArrayLen(0) },
	}
}

// Next returns the next set of keys and values.
func (c *UnsignedArrayCursorMock) Next() *tsdb.UnsignedArray {
	return c.NextFn()
}

// StringArrayCursorMock provides a mock implementation of a StringArrayCursor.
type StringArrayCursorMock struct {
	*ArrayCursorMock
	NextFn func() *tsdb.StringArray
}

// NewStringArrayCursorMock returns an initialised StringArrayCursorMock, which
// returns the zero value for all methods.
func NewStringArrayCursorMock() *StringArrayCursorMock {
	return &StringArrayCursorMock{
		ArrayCursorMock: NewArrayCursorMock(),
		NextFn:          func() *tsdb.StringArray { return tsdb.NewStringArrayLen(0) },
	}
}

// Next returns the next set of keys and values.
func (c *StringArrayCursorMock) Next() *tsdb.StringArray {
	return c.NextFn()
}

// BooleanArrayCursorMock provides a mock implementation of a BooleanArrayCursor.
type BooleanArrayCursorMock struct {
	*ArrayCursorMock
	NextFn func() *tsdb.BooleanArray
}

// NewBooleanArrayCursorMock returns an initialised BooleanArrayCursorMock, which
// returns the zero value for all methods.
func NewBooleanArrayCursorMock() *BooleanArrayCursorMock {
	return &BooleanArrayCursorMock{
		ArrayCursorMock: NewArrayCursorMock(),
		NextFn:          func() *tsdb.BooleanArray { return tsdb.NewBooleanArrayLen(0) },
	}
}

// Next returns the next set of keys and values.
func (c *BooleanArrayCursorMock) Next() *tsdb.BooleanArray {
	return c.NextFn()
}
