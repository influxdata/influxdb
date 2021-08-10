package report_tsi

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type cmdParams struct {
	bucketId     string
	concurrency  int
	dataPath     string
	topN         int
	expectedOut  string
	expectedOuts []string
	expectErr    bool
}

func Test_ReportTSI_Missing_BucketID(t *testing.T) {
	params := cmdParams{
		expectErr:   true,
		expectedOut: "required flag(s) \"bucket-id\" not set",
	}
	runCommand(t, params)
}

func Test_ReportTSI_Bucket_Does_Not_Exist(t *testing.T) {
	params := cmdParams{
		bucketId:    "12345",
		expectErr:   true,
		expectedOut: "open 12345/autogen: no such file or directory",
	}
	runCommand(t, params)
}

func Test_ReportTSI_Bucket_Contains_No_Shards(t *testing.T) {
	path := newTempDirectories(t, false)
	defer os.RemoveAll(path)
	params := cmdParams{
		bucketId:    "12345",
		dataPath:    path,
		expectedOut: "No shards under " + filepath.Join(path, "12345", "autogen"),
	}
	runCommand(t, params)
}

func Test_ReportTSI_Invalid_Index_Dir(t *testing.T) {
	path := newTempDirectories(t, true)
	defer os.RemoveAll(path)
	params := cmdParams{
		bucketId:    "12345",
		dataPath:    path,
		expectErr:   true,
		expectedOut: fmt.Sprintf("not a TSI index directory: %s", filepath.Join(path, "12345", "autogen", "1", "index")),
	}
	runCommand(t, params)
}

func Test_ReportTSI_Valid_No_Roaring_Bitmap(t *testing.T) {
	params := cmdParams{
		bucketId:    "test-db-low-cardinality",
		dataPath:    "../tsi-test-data",
		concurrency: 1,
		expectedOuts: []string{
			"Summary\nDatabase Path: ../tsi-test-data/test-db-low-cardinality\nCardinality (exact): 5",
			"Shard ID: 1\nPath: ../tsi-test-data/test-db-low-cardinality/autogen/1\nCardinality (exact): 5",
			"\"m0\"\t1\t\n\"m1\"\t1\t\n\"m2\"\t1\t\n\"m3\"\t1\t\n\"m4\"\t1\t",
		},
	}
	runCommand(t, params)
}

func Test_ReportTSI_Valid_Roaring_Bitmap(t *testing.T) {
	params := cmdParams{
		bucketId:    "test-db-high-cardinality",
		dataPath:    "../tsi-test-data",
		concurrency: 1,
		expectedOuts: []string{
			"Summary\nDatabase Path: ../tsi-test-data/test-db-high-cardinality\nCardinality (exact): 31",
			"Shard ID: 1\nPath: ../tsi-test-data/test-db-high-cardinality/autogen/1\nCardinality (exact): 31",
			"\"m0\"\t27\t\n\"m1\"\t1\t\n\"m2\"\t1\t\n\"m3\"\t1\t\n\"m4\"\t1\t",
		},
	}
	runCommand(t, params)
}

func Test_ReportTSI_Valid_TopN(t *testing.T) {
	params := cmdParams{
		bucketId:    "test-db-low-cardinality",
		dataPath:    "../tsi-test-data",
		concurrency: 1,
		topN:        2,
		expectedOuts: []string{
			"Summary\nDatabase Path: ../tsi-test-data/test-db-low-cardinality\nCardinality (exact): 5",
			"Shard ID: 1\nPath: ../tsi-test-data/test-db-low-cardinality/autogen/1\nCardinality (exact): 5",
			"\"m0\"\t1\t\n\"m1\"\t1\t\n\n\n",
		},
	}
	runCommand(t, params)
}

func newTempDirectories(t *testing.T, withShards bool) string {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "reporttsi")
	require.NoError(t, err)

	err = os.MkdirAll(filepath.Join(dataDir, "12345", "autogen"), 0777)
	require.NoError(t, err)

	if withShards {
		// Create shard and index directory within it, with one partition
		err = os.MkdirAll(filepath.Join(dataDir, "12345", "autogen", "1", "index", "0"), 0777)
		require.NoError(t, err)
	}

	return dataDir
}

func initCommand(t *testing.T, params cmdParams) *cobra.Command {
	t.Helper()

	// Creates new command and sets args
	cmd := NewReportTSICommand()

	allArgs := make([]string, 0)

	if params.bucketId != "" {
		allArgs = append(allArgs, "--bucket-id", params.bucketId)
	}

	if params.concurrency != runtime.GOMAXPROCS(0) {
		allArgs = append(allArgs, "--concurrency", strconv.Itoa(params.concurrency))
	}

	if params.dataPath != os.Getenv("HOME")+"/.influxdbv2/engine/data" {
		allArgs = append(allArgs, "--data-path", params.dataPath)
	}

	if params.topN != 0 {
		allArgs = append(allArgs, "--top", strconv.Itoa(params.topN))
	}

	cmd.SetArgs(allArgs)

	return cmd
}

func getOutput(t *testing.T, cmd *cobra.Command) []byte {
	t.Helper()

	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.SetErr(b)
	require.NoError(t, cmd.Execute())

	out, err := io.ReadAll(b)
	require.NoError(t, err)

	return out
}

func runCommand(t *testing.T, params cmdParams) {
	t.Helper()

	cmd := initCommand(t, params)

	if params.expectErr {
		require.EqualError(t, cmd.Execute(), params.expectedOut)
		return
	}

	// Get output
	out := getOutput(t, cmd)

	// Check output
	if params.expectedOut != "" {
		require.Contains(t, string(out), params.expectedOut)
	} else {
		for _, output := range params.expectedOuts {
			require.Contains(t, string(out), output)
		}
	}
}
