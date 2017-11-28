package tsm1

import (
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/uber-go/zap"
)

type testFinalizerIterator struct {
	OnClose func()
}

func (itr *testFinalizerIterator) Next() (*influxql.FloatPoint, error) {
	return nil, nil
}

func (itr *testFinalizerIterator) Close() error {
	// Act as if this is a slow finalizer and ensure that it doesn't block
	// the finalizer background thread.
	itr.OnClose()
	return nil
}

func (itr *testFinalizerIterator) Stats() influxql.IteratorStats {
	return influxql.IteratorStats{}
}

func TestFinalizerIterator(t *testing.T) {
	var (
		step1 = make(chan struct{})
		step2 = make(chan struct{})
		step3 = make(chan struct{})
	)

	l := zap.New(zap.NewTextEncoder(), zap.Output(os.Stderr))
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
