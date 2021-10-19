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

	"github.com/influxdata/influxdb/v2/pkg/tar"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type cmdParams struct {
	testName     string
	bucketId     string
	concurrency  int
	dataPath     string
	topN         int
	expectedOut  string
	expectedOuts []string
	expectErr    bool
}

const (
	bucketID = "12345"
	lowCard  = "test-db-low-cardinality"
	highCard = "test-db-high-cardinality"
)

func Test_ReportTSI_GeneratedData(t *testing.T) {
	shardlessPath := newTempDirectories(t, false)
	defer os.RemoveAll(shardlessPath)

	shardPath := newTempDirectories(t, true)
	defer os.RemoveAll(shardPath)

	tests := []cmdParams{
		{
			testName:    "Bucket_Does_Not_Exist",
			expectErr:   true,
			expectedOut: fmt.Sprintf("open %s", filepath.Join(bucketID, "autogen")),
		},
		{
			testName:    "Bucket_Contains_No_Shards",
			dataPath:    shardlessPath,
			expectedOut: fmt.Sprintf("No shards under %s", filepath.Join(shardlessPath, bucketID, "autogen")),
		},
		{
			testName:    "Invalid_Index_Dir",
			dataPath:    shardPath,
			expectErr:   true,
			expectedOut: fmt.Sprintf("not a TSI index directory: %s", filepath.Join(shardPath, bucketID, "autogen", "1", "index")),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.testName, func(t *testing.T) {
			runCommand(t, tc)
		})
	}
}

func Test_ReportTSI_TestData(t *testing.T) {

	// Create temp directory for extracted test data
	path, err := os.MkdirTemp("", "report-tsi-test-")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	// Extract test data
	file, err := os.Open("../tsi-test-data.tar.gz")
	require.NoError(t, err)
	defer file.Close()
	require.NoError(t, tar.Untar(path, file))

	tests := []cmdParams{
		{
			testName: "Valid_No_Roaring_Bitmap",
			bucketId: lowCard,
			dataPath: path,
			expectedOuts: []string{
				fmt.Sprintf("Summary\nDatabase Path: %s\nCardinality (exact): 5", filepath.Join(path, lowCard)),
				fmt.Sprintf("Shard ID: 1\nPath: %s\nCardinality (exact): 5", filepath.Join(path, lowCard, "autogen", "1")),
				"\"m0\"\t1\t\n\"m1\"\t1\t\n\"m2\"\t1\t\n\"m3\"\t1\t\n\"m4\"\t1\t",
			},
		},
		{
			testName: "Valid_Roaring_Bitmap",
			bucketId: highCard,
			dataPath: path,
			expectedOuts: []string{
				fmt.Sprintf("Summary\nDatabase Path: %s\nCardinality (exact): 31", filepath.Join(path, highCard)),
				fmt.Sprintf("Shard ID: 1\nPath: %s\nCardinality (exact): 31", filepath.Join(path, highCard, "autogen", "1")),
				"\"m0\"\t27\t\n\"m1\"\t1\t\n\"m2\"\t1\t\n\"m3\"\t1\t\n\"m4\"\t1\t",
			},
		},
		{
			testName: "Valid_TopN",
			bucketId: lowCard,
			dataPath: path,
			topN: 2,
			expectedOuts: []string{
				fmt.Sprintf("Summary\nDatabase Path: %s\nCardinality (exact): 5", filepath.Join(path, lowCard)),
				fmt.Sprintf("Shard ID: 1\nPath: %s\nCardinality (exact): 5", filepath.Join(path, lowCard, "autogen", "1")),
				"\"m0\"\t1\t\n\"m1\"\t1\t\n\n\n",
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.testName, func(t *testing.T) {
			tc.concurrency = 1
			runCommand(t, tc)
		})
	}
}

func newTempDirectories(t *testing.T, withShards bool) string {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "reporttsi")
	require.NoError(t, err)

	err = os.MkdirAll(filepath.Join(dataDir, bucketID, "autogen"), 0777)
	require.NoError(t, err)

	if withShards {
		// Create shard and index directory within it, with one partition
		err = os.MkdirAll(filepath.Join(dataDir, bucketID, "autogen", "1", "index", "0"), 0777)
		require.NoError(t, err)
	}

	return dataDir
}

func initCommand(t *testing.T, params cmdParams) *cobra.Command {
	t.Helper()

	// Creates new command and sets args
	cmd := NewReportTSICommand()

	allArgs := make([]string, 0)

	if params.bucketId == "" {
		allArgs = append(allArgs, "--bucket-id", bucketID)
	} else {
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

	b := &bytes.Buffer{}
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
		require.Contains(t, cmd.Execute().Error(), params.expectedOut)
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
