package helper

import (
	"io"
	"sync"

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
	return func() {
		if h, ok := t.(interface{ Helper() }); ok {
			h.Helper()
		}

		require.NoError(t, c.Close())
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
	innerCloser := CheckedClose(t, c)
	var o sync.Once
	return func() {
		o.Do(innerCloser)
	}
}
