package dumptsi_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/cmd/influx_inspect/dumptsi"
	"github.com/influxdata/influxdb/pkg/tar"
	"github.com/stretchr/testify/require"
)

func Test_DumpTSI_NoError(t *testing.T) {

	// Create the Command object
	cmd := dumptsi.NewCommand()
	b := bytes.NewBufferString("")
	cmd.Stdout = b

	// Create the temp-dir for our un-tared files to live in
	dir, err := ioutil.TempDir("", "dumptsitest-")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Untar the test data
	file, err := os.Open("./testdata.tar.gz")
	require.NoError(t, err)
	require.NoError(t, tar.Untar(dir, file))
	require.NoError(t, file.Close())

	// Run the test
	require.NoError(t, cmd.Run(
		"--series-file", dir+string(os.PathSeparator)+"_series",
		dir+string(os.PathSeparator)+"L0-00000001.tsl",
	))

	// Validate output is as-expected
	out := b.String()
	require.Contains(t, out, "[LOG FILE] L0-00000001.tsl")
	require.Contains(t, out, "Series:\t\t9")
	require.Contains(t, out, "Measurements:\t6")
	require.Contains(t, out, "Tag Keys:\t18")
	require.Contains(t, out, "Tag Values:\t26")
}
