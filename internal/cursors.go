package internal

import "github.com/influxdata/influxdb/tsdb"

var (
	_ tsdb.IntegerBatchCursor  = NewIntegerBatchCursorMock()
	_ tsdb.FloatBatchCursor    = NewFloatBatchCursorMock()
	_ tsdb.UnsignedBatchCursor = NewUnsignedBatchCursorMock()
	_ tsdb.StringBatchCursor   = NewStringBatchCursorMock()
	_ tsdb.BooleanBatchCursor  = NewBooleanBatchCursorMock()
)

// BatchCursorMock provides a mock base implementation for batch cursors.
type BatchCursorMock struct {
	CloseFn func()
	ErrFn   func() error
}

// NewBatchCursorMock returns an initialised BatchCursorMock, which
// returns the zero value for all methods.
func NewBatchCursorMock() *BatchCursorMock {
	return &BatchCursorMock{
		CloseFn: func() {},
		ErrFn:   func() error { return nil },
	}
}

// Close closes the cursor.
func (c *BatchCursorMock) Close() { c.CloseFn() }

// Err returns the latest error, if any.
func (c *BatchCursorMock) Err() error { return c.ErrFn() }

// IntegerBatchCursorMock provides a mock implementation of an IntegerBatchCursorMock.
type IntegerBatchCursorMock struct {
	*BatchCursorMock
	NextFn func() (keys []int64, values []int64)
}

// NewIntegerBatchCursorMock returns an initialised IntegerBatchCursorMock, which
// returns the zero value for all methods.
func NewIntegerBatchCursorMock() *IntegerBatchCursorMock {
	return &IntegerBatchCursorMock{
		BatchCursorMock: NewBatchCursorMock(),
		NextFn:          func() ([]int64, []int64) { return nil, nil },
	}
}

// Next returns the next set of keys and values.
func (c *IntegerBatchCursorMock) Next() (keys []int64, values []int64) {
	return c.NextFn()
}

// FloatBatchCursorMock provides a mock implementation of a FloatBatchCursor.
type FloatBatchCursorMock struct {
	*BatchCursorMock
	NextFn func() (keys []int64, values []float64)
}

// NewFloatBatchCursorMock returns an initialised FloatBatchCursorMock, which
// returns the zero value for all methods.
func NewFloatBatchCursorMock() *FloatBatchCursorMock {
	return &FloatBatchCursorMock{
		BatchCursorMock: NewBatchCursorMock(),
		NextFn:          func() ([]int64, []float64) { return nil, nil },
	}
}

// Next returns the next set of keys and values.
func (c *FloatBatchCursorMock) Next() (keys []int64, values []float64) {
	return c.NextFn()
}

// UnsignedBatchCursorMock provides a mock implementation of an UnsignedBatchCursorMock.
type UnsignedBatchCursorMock struct {
	*BatchCursorMock
	NextFn func() (keys []int64, values []uint64)
}

// NewUnsignedBatchCursorMock returns an initialised UnsignedBatchCursorMock, which
// returns the zero value for all methods.
func NewUnsignedBatchCursorMock() *UnsignedBatchCursorMock {
	return &UnsignedBatchCursorMock{
		BatchCursorMock: NewBatchCursorMock(),
		NextFn:          func() ([]int64, []uint64) { return nil, nil },
	}
}

// Next returns the next set of keys and values.
func (c *UnsignedBatchCursorMock) Next() (keys []int64, values []uint64) {
	return c.NextFn()
}

// StringBatchCursorMock provides a mock implementation of a StringBatchCursor.
type StringBatchCursorMock struct {
	*BatchCursorMock
	NextFn func() (keys []int64, values []string)
}

// NewStringBatchCursorMock returns an initialised StringBatchCursorMock, which
// returns the zero value for all methods.
func NewStringBatchCursorMock() *StringBatchCursorMock {
	return &StringBatchCursorMock{
		BatchCursorMock: NewBatchCursorMock(),
		NextFn:          func() ([]int64, []string) { return nil, nil },
	}
}

// Next returns the next set of keys and values.
func (c *StringBatchCursorMock) Next() (keys []int64, values []string) {
	return c.NextFn()
}

// BooleanBatchCursorMock provides a mock implementation of a BooleanBatchCursor.
type BooleanBatchCursorMock struct {
	*BatchCursorMock
	NextFn func() (keys []int64, values []bool)
}

// NewBooleanBatchCursorMock returns an initialised BooleanBatchCursorMock, which
// returns the zero value for all methods.
func NewBooleanBatchCursorMock() *BooleanBatchCursorMock {
	return &BooleanBatchCursorMock{
		BatchCursorMock: NewBatchCursorMock(),
		NextFn:          func() ([]int64, []bool) { return nil, nil },
	}
}

// Next returns the next set of keys and values.
func (c *BooleanBatchCursorMock) Next() (keys []int64, values []bool) {
	return c.NextFn()
}
