package reads

import (
	"testing"

	"github.com/influxdata/influxdb/v2/v1/tsdb/cursors"
)

func TestIntegerFilterArrayCursor(t *testing.T) {
	var i int
	expr := MockExpression{
		EvalBoolFunc: func(v Valuer) bool {
			i++
			return i%2 == 0
		},
	}

	var resultN int
	ac := MockIntegerArrayCursor{
		CloseFunc: func() {},
		ErrFunc:   func() error { return nil },
		StatsFunc: func() cursors.CursorStats { return cursors.CursorStats{} },
		NextFunc: func() *cursors.IntegerArray {
			resultN++
			if resultN == 4 {
				return cursors.NewIntegerArrayLen(0)
			}
			return cursors.NewIntegerArrayLen(900)
		},
	}

	c := newIntegerFilterArrayCursor(&expr)
	c.reset(&ac)

	if got, want := len(c.Next().Timestamps), 1000; got != want {
		t.Fatalf("len(Next())=%d, want %d", got, want)
	} else if got, want := len(c.Next().Timestamps), 350; got != want {
		t.Fatalf("len(Next())=%d, want %d", got, want)
	}
}

type MockIntegerArrayCursor struct {
	CloseFunc func()
	ErrFunc   func() error
	StatsFunc func() cursors.CursorStats
	NextFunc  func() *cursors.IntegerArray
}

func (c *MockIntegerArrayCursor) Close()                      { c.CloseFunc() }
func (c *MockIntegerArrayCursor) Err() error                  { return c.ErrFunc() }
func (c *MockIntegerArrayCursor) Stats() cursors.CursorStats  { return c.StatsFunc() }
func (c *MockIntegerArrayCursor) Next() *cursors.IntegerArray { return c.NextFunc() }

type MockExpression struct {
	EvalBoolFunc func(v Valuer) bool
}

func (e *MockExpression) EvalBool(v Valuer) bool { return e.EvalBoolFunc(v) }
