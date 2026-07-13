package tsm1

import (
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"github.com/stretchr/testify/require"
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

// BenchmarkIntegerIterator_Next_Condition exercises the per-point condition
// evaluation (itr.valuer.EvalBool) for a WHERE-filtered query that does NOT use
// date_part. This is the common path: with opt.NeedTimeRef false the DatePartValuer
// must be left out of the eval chain so no extra valuer indirection is paid per
// scanned point.
func BenchmarkIntegerIterator_Next_Condition(b *testing.B) {
	opt := query.IteratorOptions{
		Aux:       []influxql.VarRef{{Val: "f1", Type: influxql.Integer}},
		Condition: influxql.MustParseExpr("f1 > 0"),
		// NeedTimeRef defaults to false: the condition has no date_part.
	}
	aux := []cursorAt{&literalValueCursor{value: int64(1e3)}}
	conds := []cursorAt{&literalValueCursor{value: int64(1e3)}}
	condNames := []string{"f1"}

	cur := newIntegerIterator("m0", query.Tags{}, opt, &infiniteIntegerCursor{}, aux, conds, condNames)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cur.Next()
	}
}

// BenchmarkIntegerIterator_Next_DatePartCondition measures the per-point cost of a
// WHERE condition that uses date_part. Relative to
// BenchmarkIntegerIterator_Next_Condition it adds the once-per-iterator condition
// rewrite's per-point work: extracting the referenced parts and publishing them to
// the eval map through the boxing cache. Timestamps advance one second per point:
// boxing a large int64 into the eval map allocates, while a constant time of zero
// would hit the runtime's small-integer cache and understate the cost.
func BenchmarkIntegerIterator_Next_DatePartCondition(b *testing.B) {
	opt := query.IteratorOptions{
		Aux:         []influxql.VarRef{{Val: "f1", Type: influxql.Integer}},
		Condition:   influxql.MustParseExpr("f1 > 0 AND date_part('hour', time) < 12"),
		NeedTimeRef: true,
		EndTime:     influxql.MaxTime,
		Ascending:   true,
	}
	aux := []cursorAt{&literalValueCursor{value: int64(1e3)}}
	conds := []cursorAt{&literalValueCursor{value: int64(1e3)}}
	condNames := []string{"f1"}

	cur := newIntegerIterator("m0", query.Tags{}, opt, &advancingIntegerCursor{}, aux, conds, condNames)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cur.Next()
	}
}

// BenchmarkIntegerIterator_Next_DatePartDimension measures the per-point cost of a
// GROUP BY date_part dimension: the dimension value is extracted from the point
// timestamp and written into the trailing Aux slot for every point the iterator
// returns.
func BenchmarkIntegerIterator_Next_DatePartDimension(b *testing.B) {
	opt := query.IteratorOptions{
		Aux: []influxql.VarRef{
			{Val: "f1", Type: influxql.Integer},
			{Val: "hour", Type: influxql.Integer},
		},
		DatePartDimensions: []query.DatePartDimension{{Expr: query.Hour}},
		EndTime:            influxql.MaxTime,
		Ascending:          true,
	}
	aux := []cursorAt{
		&literalValueCursor{value: int64(1e3)},
		// The dimension slot is overwritten with the extracted date_part value.
		&literalValueCursor{value: nil},
	}

	cur := newIntegerIterator("m0", query.Tags{}, opt, &advancingIntegerCursor{}, aux, nil, nil)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cur.Next()
	}
}

// TestIntegerIterator_Next_DatePartCondition_ZeroAllocs pins the alloc-free
// date_part condition path: the condition is rewritten at construction and the
// per-point part values are published through a boxing cache, so steady-state
// scanning must not allocate. Uses advancing timestamps within a single hour so
// the cached boxed value stays valid.
func TestIntegerIterator_Next_DatePartCondition_ZeroAllocs(t *testing.T) {
	opt := query.IteratorOptions{
		Aux:         []influxql.VarRef{{Val: "f1", Type: influxql.Integer}},
		Condition:   influxql.MustParseExpr("f1 > 0 AND date_part('hour', time) < 12"),
		NeedTimeRef: true,
		EndTime:     influxql.MaxTime,
		Ascending:   true,
	}
	aux := []cursorAt{&literalValueCursor{value: int64(1e3)}}
	conds := []cursorAt{&literalValueCursor{value: int64(1e3)}}

	cur := newIntegerIterator("m0", query.Tags{}, opt, &advancingIntegerCursor{}, aux, conds, []string{"f1"})

	_, err := cur.Next() // prime the boxing cache
	require.NoError(t, err)

	allocs := testing.AllocsPerRun(1000, func() {
		cur.Next()
	})
	require.Zero(t, allocs)
}

// TestIntegerIterator_Next_NeedTimeRef_NilCondition guards against a nil-map panic
// when an IteratorOptions arrives with NeedTimeRef=true but Condition=nil. Locally
// conditionNeedsTimeRef(nil) keeps the invariant (NeedTimeRef implies a condition),
// but the enterprise wire codec encodes the two fields independently and could
// deliver this combination; itr.m must still be allocated before the time-ref write.
func TestIntegerIterator_Next_NeedTimeRef_NilCondition(t *testing.T) {
	opt := query.IteratorOptions{
		Aux:         []influxql.VarRef{{Val: "f1", Type: influxql.Integer}},
		NeedTimeRef: true,
		// Condition is intentionally nil.
	}
	aux := []cursorAt{&literalValueCursor{value: int64(1e3)}}

	cur := newIntegerIterator("m0", query.Tags{}, opt, &infiniteIntegerCursor{}, aux, nil, nil)

	require.NotPanics(t, func() {
		_, err := cur.Next()
		require.NoError(t, err)
	})
}

// advancingIntegerCursor returns points whose timestamps advance one second per
// call, so per-point time handling boxes realistic (large) int64 values.
type advancingIntegerCursor struct {
	t int64
}

func (*advancingIntegerCursor) close() error {
	return nil
}

func (c *advancingIntegerCursor) next() (t int64, v interface{}) {
	return c.nextInteger()
}

func (c *advancingIntegerCursor) nextInteger() (t int64, v int64) {
	c.t += int64(time.Second)
	return c.t, 0
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
