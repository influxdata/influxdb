package dump_tsi_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/v2/cmd/influxd/inspect/dump_tsi"
	"github.com/influxdata/influxdb/v2/pkg/tar"
	"github.com/stretchr/testify/require"
)

func Test_DumpTSI_NoError(t *testing.T) {

	// Create the Command object
	cmd := dump_tsi.NewDumpTSICommand()
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	// Create the temp-dir for our un-tared files to live in
	dir, err := os.MkdirTemp("", "dumptsitest-")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Untar the test data
	file, err := os.Open("../tsi-test-data.tar.gz")
	require.NoError(t, err)
	defer file.Close()
	require.NoError(t, tar.Untar(dir, file))

	// Run the test
	cmd.SetArgs([]string{
		"--series-file", filepath.Join(dir, "test-db-low-cardinality", "_series"),
		filepath.Join(dir, "test-db-low-cardinality", "autogen", "1", "index", "0", "L0-00000001.tsl"),
	})
	require.NoError(t, cmd.Execute())

	// Validate output is as-expected
	out := b.String()
	require.Contains(t, out, "[LOG FILE] L0-00000001.tsl")
	require.Contains(t, out, "Series:\t\t1")
	require.Contains(t, out, "Measurements:\t1")
	require.Contains(t, out, "Tag Keys:\t6")
	require.Contains(t, out, "Tag Values:\t6")
}
