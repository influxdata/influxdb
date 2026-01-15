package helper

import (
	"errors"
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
type mockTestingT struct {
	failed     bool
	helperUsed bool
}

func (m *mockTestingT) Errorf(format string, args ...any) {
	m.failed = true
}

func (m *mockTestingT) FailNow() {
	m.failed = true
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
