package helper

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockCloser is an io.Closer that returns a configurable error.
type mockCloser struct {
	err    error
	closed bool
}

func (m *mockCloser) Close() error {
	m.closed = true
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
