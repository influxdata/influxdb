package helper

import (
	"io"

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
