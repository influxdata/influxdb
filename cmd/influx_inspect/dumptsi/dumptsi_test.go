package dumptsi_test

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/cmd/influx_inspect/dumptsi"
	"github.com/stretchr/testify/require"
)

func Test_DumpTSI_NoError(t *testing.T) {
	cmd := dumptsi.NewCommand()
	b := bytes.NewBufferString("")
	cmd.Stdout = b
	require.NoError(t, cmd.Run(
		"--series-file", "./testdata/_series",
		"./testdata/L0-00000001.tsl",
	))
	out := b.String()
	require.Contains(t, out, "[LOG FILE] L0-00000001.tsl")
	require.Contains(t, out, "Series:\t\t9")
	require.Contains(t, out, "Measurements:\t6")
	require.Contains(t, out, "Tag Keys:\t18")
	require.Contains(t, out, "Tag Values:\t26")
}
