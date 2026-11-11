package helper

import (
	"io"
	"sync"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CheckedClose returns a function that can be used with a defer to check that a
// Closer's Close method does not return an error.
//
// By using CheckedClose, the following code:
// defer func() { require.NoError(t, c.Close()) }()
// can be replaced by:
// defer CheckedClose(t, c)()
func CheckedClose(t require.TestingT, c io.Closer) func() {
	closer := AssertCheckedClose(t, c)
	return func() {
		if h, ok := t.(interface{ Helper() }); ok {
			h.Helper()
		}

		if !closer() {
			t.FailNow()
		}
	}
}

// AssertCheckedClose returns a function that can be used with a defer to
// check that a Closer's Close method does not return an error. It
// is identical to CheckedClose, but uses assert instead of require.
//
// This is primarily useful for situations that allow assert but not
// require, such as require.EventuallyWithT.
func AssertCheckedClose(t assert.TestingT, c io.Closer) func() bool {
	return func() bool {
		if h, ok := t.(interface{ Helper() }); ok {
			h.Helper()
		}

		return assert.NoError(t, c.Close())
	}
}

// CheckedCloseOnce returns a function that will call Close on c and check
// for errors. The returned function has no effect after being called the first
// time. It can be used to replace code like the following:
//
//	c := NewSomeCloser()
//	defer func() {
//		if c != nil {
//			require.NoError(c.Close())
//			c = nil
//		}
//	}()
//	...
//	require.NoError(t, c.Close())
//	c = nil
//
// Example:
//
//	closer := CheckedCloseOnce(t, c)
//	defer closer()
//	...
//	closer()
func CheckedCloseOnce(t require.TestingT, c io.Closer) func() {
	var o sync.Once
	return func() {
		if h, ok := t.(interface{ Helper() }); ok {
			h.Helper()
		}

		// Close inside the Once so that Do only returns after the close has
		// finished, but check the result outside it so that a failure is
		// attributed to the caller rather than to sync.Once.
		ran := false
		var err error
		o.Do(func() {
			ran = true
			err = c.Close()
		})
		if ran {
			require.NoError(t, err)
		}
	}
}
