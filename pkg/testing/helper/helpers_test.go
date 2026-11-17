package helper

import (
	"errors"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockCloser is an io.Closer that returns a configurable error.
type mockCloser struct {
	err        error
	closed     bool
	closeCount int
}

func (m *mockCloser) Close() error {
	m.closed = true
	m.closeCount++
	return m.err
}

// mockTestingT implements require.TestingT for testing CheckedClose behavior.
// It counts Errorf and FailNow separately so tests can distinguish assert
// semantics (report only) from require semantics (report, then stop).
type mockTestingT struct {
	failed       bool
	helperUsed   bool
	errorfCalls  int
	failNowCalls int
}

func (m *mockTestingT) Errorf(format string, args ...any) {
	m.failed = true
	m.errorfCalls++
}

func (m *mockTestingT) FailNow() {
	m.failed = true
	m.failNowCalls++
}

func (m *mockTestingT) Helper() {
	m.helperUsed = true
}

func TestCheckedClose(t *testing.T) {
	t.Run("returns a function", func(t *testing.T) {
		closer := &mockCloser{}
		fn := CheckedClose(t, closer)
		require.NotNil(t, fn)
	})

	t.Run("calls Close on the closer", func(t *testing.T) {
		closer := &mockCloser{}
		fn := CheckedClose(t, closer)
		fn()
		require.True(t, closer.closed)
	})

	t.Run("does not fail when Close returns nil", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{err: nil}
		fn := CheckedClose(mockT, closer)
		fn()
		require.False(t, mockT.failed)
	})

	t.Run("fails when Close returns an error", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{err: errors.New("close error")}
		fn := CheckedClose(mockT, closer)
		fn()
		require.True(t, mockT.failed)
	})

	t.Run("reports once and then calls FailNow when Close returns an error", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{err: errors.New("close error")}
		fn := CheckedClose(mockT, closer)
		fn()
		require.Equal(t, 1, mockT.errorfCalls)
		require.Equal(t, 1, mockT.failNowCalls)
	})

	t.Run("does not report or call FailNow when Close returns nil", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{err: nil}
		fn := CheckedClose(mockT, closer)
		fn()
		require.Zero(t, mockT.errorfCalls)
		require.Zero(t, mockT.failNowCalls)
	})

	t.Run("calls Helper when available", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{}
		fn := CheckedClose(mockT, closer)
		fn()
		require.True(t, mockT.helperUsed)
	})

	t.Run("works with defer", func(t *testing.T) {
		closer := &mockCloser{}
		func() {
			defer CheckedClose(t, closer)()
			require.False(t, closer.closed, "should not be closed yet")
		}()
		require.True(t, closer.closed, "should be closed after function returns")
	})
}

func TestAssertCheckedClose(t *testing.T) {
	t.Run("returns a function", func(t *testing.T) {
		closer := &mockCloser{}
		fn := AssertCheckedClose(t, closer)
		require.NotNil(t, fn)
	})

	t.Run("calls Close on the closer", func(t *testing.T) {
		closer := &mockCloser{}
		fn := AssertCheckedClose(t, closer)
		fn()
		require.True(t, closer.closed)
	})

	t.Run("returns true and does not report when Close returns nil", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{err: nil}
		fn := AssertCheckedClose(mockT, closer)
		require.True(t, fn())
		require.False(t, mockT.failed)
		require.Zero(t, mockT.errorfCalls)
		require.Zero(t, mockT.failNowCalls)
	})

	t.Run("returns false and reports without FailNow when Close returns an error", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{err: errors.New("close error")}
		fn := AssertCheckedClose(mockT, closer)
		require.False(t, fn())
		require.True(t, mockT.failed)
		require.Equal(t, 1, mockT.errorfCalls)
		require.Zero(t, mockT.failNowCalls, "assert form must not stop the test")
	})

	t.Run("calls Helper when available", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{}
		fn := AssertCheckedClose(mockT, closer)
		fn()
		require.True(t, mockT.helperUsed)
	})

	t.Run("works with defer", func(t *testing.T) {
		closer := &mockCloser{}
		func() {
			defer AssertCheckedClose(t, closer)()
			require.False(t, closer.closed, "should not be closed yet")
		}()
		require.True(t, closer.closed, "should be closed after function returns")
	})
}

func TestCheckedCloseOnce(t *testing.T) {
	t.Run("returns a function", func(t *testing.T) {
		closer := &mockCloser{}
		fn := CheckedCloseOnce(t, closer)
		require.NotNil(t, fn)
	})

	t.Run("calls Close on the closer", func(t *testing.T) {
		closer := &mockCloser{}
		fn := CheckedCloseOnce(t, closer)
		fn()
		require.True(t, closer.closed)
	})

	t.Run("does not fail when Close returns nil", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{err: nil}
		fn := CheckedCloseOnce(mockT, closer)
		fn()
		require.False(t, mockT.failed)
	})

	t.Run("fails when Close returns an error", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{err: errors.New("close error")}
		fn := CheckedCloseOnce(mockT, closer)
		fn()
		require.True(t, mockT.failed)
	})

	t.Run("calls Helper when available", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{}
		fn := CheckedCloseOnce(mockT, closer)
		fn()
		require.True(t, mockT.helperUsed)
	})

	t.Run("works with defer", func(t *testing.T) {
		closer := &mockCloser{}
		func() {
			defer CheckedCloseOnce(t, closer)()
			require.False(t, closer.closed, "should not be closed yet")
		}()
		require.True(t, closer.closed, "should be closed after function returns")
	})

	t.Run("only closes once when called multiple times", func(t *testing.T) {
		closer := &mockCloser{}
		fn := CheckedCloseOnce(t, closer)
		fn()
		fn()
		fn()
		require.Equal(t, 1, closer.closeCount)
	})

	t.Run("reports once and then calls FailNow when Close returns an error", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{err: errors.New("close error")}
		fn := CheckedCloseOnce(mockT, closer)
		fn()
		require.Equal(t, 1, mockT.errorfCalls)
		require.Equal(t, 1, mockT.failNowCalls)
	})

	t.Run("does not repeat a failed close", func(t *testing.T) {
		mockT := &mockTestingT{}
		closer := &mockCloser{err: errors.New("close error")}
		fn := CheckedCloseOnce(mockT, closer)
		fn()
		fn()
		require.Equal(t, 1, closer.closeCount)
		require.Equal(t, 1, mockT.errorfCalls)
		require.Equal(t, 1, mockT.failNowCalls)
	})

	t.Run("works with defer and explicit call", func(t *testing.T) {
		closer := &mockCloser{}
		func() {
			closeFn := CheckedCloseOnce(t, closer)
			defer closeFn()
			closeFn() // explicit early close
			require.Equal(t, 1, closer.closeCount, "should be closed exactly once")
		}()
		require.Equal(t, 1, closer.closeCount, "should still be closed exactly once after defer")
	})
}

// attributingT reproduces how the testing package decides where to report a
// failure. Every caller of Helper is recorded, and when Errorf is called the
// stack is walked from the report upward until the first function that never
// called Helper. That frame is where a real *testing.T would attribute the
// failure, so a helper that lets a non-helper frame (for example from the sync
// package) sit between the caller and the check is caught here.
type attributingT struct {
	helpers    map[string]bool
	attributed runtime.Frame
	reported   bool
}

func newAttributingT() *attributingT {
	return &attributingT{helpers: map[string]bool{}}
}

func (a *attributingT) Helper() {
	// Skip runtime.Callers and Helper itself, then resolve through
	// CallersFrames so that an inlined caller still reports its own function.
	var pcs [1]uintptr
	if runtime.Callers(2, pcs[:]) == 0 {
		return
	}
	fr, _ := runtime.CallersFrames(pcs[:]).Next()
	a.helpers[fr.Function] = true
}

func (a *attributingT) Errorf(string, ...any) {
	a.reported = true
	pcs := make([]uintptr, 64)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		fr, more := frames.Next()
		if !a.helpers[fr.Function] {
			a.attributed = fr
			return
		}
		if !more {
			return
		}
	}
}

func (a *attributingT) FailNow() {}

func TestCloseFailureAttribution(t *testing.T) {
	failingCloser := func() *mockCloser {
		return &mockCloser{err: errors.New("close error")}
	}
	// here returns the file and line of the statement that calls it, so each
	// case can state its expected line relative to the call that follows.
	here := func() (string, int) {
		_, file, line, _ := runtime.Caller(1)
		return file, line
	}
	requireAttributedTo := func(t *testing.T, a *attributingT, file string, line int) {
		t.Helper()
		require.True(t, a.reported, "the failing close was not reported")
		require.Equal(t, file, a.attributed.File, "attributed to %s", a.attributed.Function)
		require.Equal(t, line, a.attributed.Line, "attributed to %s", a.attributed.Function)
	}

	t.Run("CheckedClose", func(t *testing.T) {
		a := newAttributingT()
		file, line := here()
		CheckedClose(a, failingCloser())()
		requireAttributedTo(t, a, file, line+1)
	})

	t.Run("AssertCheckedClose", func(t *testing.T) {
		a := newAttributingT()
		file, line := here()
		AssertCheckedClose(a, failingCloser())()
		requireAttributedTo(t, a, file, line+1)
	})

	t.Run("CheckedCloseOnce", func(t *testing.T) {
		a := newAttributingT()
		file, line := here()
		CheckedCloseOnce(a, failingCloser())()
		requireAttributedTo(t, a, file, line+1)
	})

	t.Run("CheckedCloseOnce deferred", func(t *testing.T) {
		a := newAttributingT()
		file, line := here()
		func() { defer CheckedCloseOnce(a, failingCloser())() }()
		requireAttributedTo(t, a, file, line+1)
	})

	t.Run("mock attributes to a wrapper that does not call Helper", func(t *testing.T) {
		// Control for the mock itself: a wrapper that never calls Helper must
		// be the attributed frame, not the line that invokes the wrapper.
		a := newAttributingT()
		file, line := here()
		wrapper := func() { CheckedClose(a, failingCloser())() }
		wrapper()
		requireAttributedTo(t, a, file, line+1)
	})
}
