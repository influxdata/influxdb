package check

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResponse_HasCheck(t *testing.T) {
	r := Response{
		Checks: Responses{
			{Name: "a"},
			{Name: "b"},
		},
	}
	require.True(t, r.HasCheck("a"))
	require.True(t, r.HasCheck("b"))
	require.False(t, r.HasCheck("c"))
	require.False(t, r.HasCheck(""))

	empty := Response{}
	require.False(t, empty.HasCheck("a"))
}
