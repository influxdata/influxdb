package tabwriter_test

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/v2/internal/tabwriter"
	"github.com/stretchr/testify/require"
)

func Test_WriteHeaders(t *testing.T) {
	out := bytes.Buffer{}
	w := tabwriter.NewTabWriter(&out, false)
	require.NoError(t, w.WriteHeaders("foo", "bar", "baz"))
	require.NoError(t, w.Flush())
	require.Equal(t, "foo\tbar\tbaz\n", out.String())
}

func Test_WriteHeadersDisabled(t *testing.T) {
	out := bytes.Buffer{}
	w := tabwriter.NewTabWriter(&out, true)
	require.NoError(t, w.WriteHeaders("foo", "bar", "baz"))
	require.NoError(t, w.Flush())
	require.Empty(t, out.String())
}

func Test_Write(t *testing.T) {
	out := bytes.Buffer{}
	w := tabwriter.NewTabWriter(&out, true)
	require.NoError(t, w.WriteHeaders("foo", "bar", "baz"))
	require.NoError(t, w.Write(map[string]interface{}{
		"bar": 123,
		"foo": "a string!",
		"baz": false,
	}))
	require.NoError(t, w.Flush())
	require.Equal(t, "a string!\t123\tfalse\n", out.String())
}
