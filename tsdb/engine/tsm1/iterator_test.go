package tsm1

import (
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

func BenchmarkIntegerIterator_Next(b *testing.B) {
	opt := query.IteratorOptions{
		Aux: []influxql.VarRef{{Val: "f1"}, {Val: "f1"}, {Val: "f1"}, {Val: "f1"}},
	}
	aux := []cursorAt{
		&literalValueCursor{value: "foo bar"},
		&literalValueCursor{value: int64(1e3)},
		&literalValueCursor{value: float64(1e3)},
		&literalValueCursor{value: true},
	}

	cur := newIntegerIterator("m0", query.Tags{}, opt, &infiniteIntegerCursor{}, aux, nil, nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cur.Next()
	}
}

type infiniteIntegerCursor struct{}

func (*infiniteIntegerCursor) close() error {
	return nil
}

func (*infiniteIntegerCursor) next() (t int64, v interface{}) {
	return 0, 0
}

func (*infiniteIntegerCursor) nextInteger() (t int64, v int64) {
	return 0, 0
}

type testFinalizerIterator struct {
	OnClose func()
}

func (itr *testFinalizerIterator) Next() (*query.FloatPoint, error) {
	return nil, nil
}

func (itr *testFinalizerIterator) Close() error {
	// Act as if this is a slow finalizer and ensure that it doesn't block
	// the finalizer background thread.
	itr.OnClose()
	return nil
}

func (itr *testFinalizerIterator) Stats() query.IteratorStats {
	return query.IteratorStats{}
}

func TestFinalizerIterator(t *testing.T) {
	var (
		step1 = make(chan struct{})
		step2 = make(chan struct{})
		step3 = make(chan struct{})
	)

	l := logger.New(os.Stderr)
	done := make(chan struct{})
	func() {
		itr := &testFinalizerIterator{
			OnClose: func() {
				// Simulate a slow closing iterator by waiting for the done channel
				// to be closed. The done channel is closed by a later finalizer.
				close(step1)
				<-done
				close(step3)
			},
		}
		newFinalizerIterator(itr, l)
	}()

	for i := 0; i < 100; i++ {
		runtime.GC()
	}

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-timer.C:
		t.Fatal("The finalizer for the iterator did not run")
		close(done)
	case <-step1:
		// The finalizer has successfully run, but should not have completed yet.
		timer.Stop()
	}

	select {
	case <-step3:
		t.Fatal("The finalizer should not have finished yet")
	default:
	}

	// Use a fake value that will be collected by the garbage collector and have
	// the finalizer close the channel. This finalizer should run after the iterator's
	// finalizer.
	value := func() int {
		foo := &struct {
			value int
		}{value: 1}
		runtime.SetFinalizer(foo, func(value interface{}) {
			close(done)
			close(step2)
		})
		return foo.value + 2
	}()
	if value < 2 {
		t.Log("This should never be output")
	}

	for i := 0; i < 100; i++ {
		runtime.GC()
	}

	timer.Reset(100 * time.Millisecond)
	select {
	case <-timer.C:
		t.Fatal("The second finalizer did not run")
	case <-step2:
		// The finalizer has successfully run and should have
		// closed the done channel.
		timer.Stop()
	}

	// Wait for step3 to finish where the closed value should be set.
	timer.Reset(100 * time.Millisecond)
	select {
	case <-timer.C:
		t.Fatal("The iterator was not finalized")
	case <-step3:
		timer.Stop()
	}
}

func TestBufCursor_DoubleClose(t *testing.T) {
	c := newBufCursor(nilCursor{}, true)
	if err := c.close(); err != nil {
		t.Fatalf("error closing: %v", err)
	}

	// This shouldn't panic
	if err := c.close(); err != nil {
		t.Fatalf("error closing: %v", err)
	}

}
