package check

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHasCheck(t *testing.T) {
	r := NewBasicResponse("", StatusPass, "", Responses{
		NamedPass("a"),
		NamedPass("b"),
	})
	require.True(t, HasCheck(r, "a"))
	require.True(t, HasCheck(r, "b"))
	require.False(t, HasCheck(r, "c"))
	require.False(t, HasCheck(r, ""))

	require.False(t, HasCheck(BasicResponse{}, "a"))
}
