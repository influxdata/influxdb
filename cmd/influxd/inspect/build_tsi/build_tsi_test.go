package build_tsi

import (
	"bytes"
	"fmt"
	"github.com/influxdata/influxdb/v2/models"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type cmdParams struct {
	dataPath          string // optional. Defaults to <engine_path>/engine/data
	walPath           string // optional. Defaults to <engine_path>/engine/wal
	bucketID          string // optional. Defaults to all buckets
	shardID           string // optional. Defaults to all shards
	batchSize         int    // optional. Defaults to 10000
	maxLogFileSize    int64  // optional. Defaults to tsdb.DefaultMaxIndexLogFileSize
	maxCacheSize      uint64 // optional. Defaults to tsdb.DefaultCacheMaxMemorySize
	compactSeriesFile bool   // optional. Defaults to false
	concurrency       int    // optional. Defaults to GOMAXPROCS(0)
	verbose           bool   // optional. Defaults to false
}

type cmdOuts struct {
	expectedOut         string
	expectedOuts        []string
	expectErr           bool
	expectBuiltIndex    bool
	expectCompactSeries bool
	sfile				*tsdb.SeriesFile
}

func Test_BuildTSI_ShardID_Without_BucketID(t *testing.T) {
	params := cmdParams{
		shardID:     "1",
		concurrency: 1,
	}

	outs := cmdOuts{
		expectErr:   true,
		expectedOut: "if shard-id is specified, bucket-id must also be specified",
	}

	runCommand(t, params, outs)
}

func Test_BuildTSI_Invalid_Compact_Series(t *testing.T) {
	params := cmdParams{
		bucketID:          "12345",
		shardID:           "1",
		concurrency:       1,
		compactSeriesFile: true,
	}

	outs := cmdOuts{
		expectErr:   true,
		expectedOut: "cannot specify shard ID when compacting series file",
	}

	runCommand(t, params, outs)
}

func Test_BuildTSI_Invalid_Index_Already_Exists(t *testing.T) {
	tempDir := newTempDirectory(t, "", "build-tsi")
	defer os.RemoveAll(tempDir)

	os.MkdirAll(filepath.Join(tempDir, "data", "12345", "autogen", "1", "index"), 0777)
	os.MkdirAll(filepath.Join(tempDir, "wal", "12345", "autogen", "1"), 0777)

	params := cmdParams{
		dataPath:    filepath.Join(tempDir, "data"),
		walPath:     filepath.Join(tempDir, "wal"),
		concurrency: 1,
	}

	outs := cmdOuts{
		expectedOut: "tsi1 index already exists, skipping",
	}

	runCommand(t, params, outs)
}

func Test_BuildTSI_Valid(t *testing.T) {
	tempDir := newTempDirectory(t, "", "build-tsi")
	defer os.RemoveAll(tempDir)

	os.MkdirAll(filepath.Join(tempDir, "data", "12345", "autogen", "1"), 0777)
	os.MkdirAll(filepath.Join(tempDir, "wal", "12345", "autogen", "1"), 0777)

	// Create a .tsm file
	f, err := os.CreateTemp(filepath.Join(tempDir, "data", "12345", "autogen", "1"), "buildtsitest*"+"."+tsm1.TSMFileExtension)
	require.NoError(t, err)

	w, err := tsm1.NewTSMWriter(f)
	require.NoError(t, err)

	values := []tsm1.Value{tsm1.NewValue(0, 1.0)}
	require.NoError(t, w.Write([]byte("cpu"), values))
	require.NoError(t, w.WriteIndex())

	w.Close()

	params := cmdParams{
		dataPath:       filepath.Join(tempDir, "data"),
		walPath:        filepath.Join(tempDir, "wal"),
		concurrency:    1,
		batchSize:      defaultBatchSize,
		maxLogFileSize: tsdb.DefaultMaxIndexLogFileSize,
		maxCacheSize:   tsdb.DefaultCacheMaxMemorySize,
	}

	outs := cmdOuts{
		expectBuiltIndex: true,
	}

	runCommand(t, params, outs)
}

func Test_BuildTSI_Valid_Verbose(t *testing.T) {
	// Set up temp directory structure
	tempDir := newTempDirectory(t, "", "build-tsi")
	defer os.RemoveAll(tempDir)

	os.MkdirAll(filepath.Join(tempDir, "data", "12345", "autogen", "1"), 0777)
	os.MkdirAll(filepath.Join(tempDir, "wal", "12345", "autogen", "1"), 0777)

	// Create temp .tsm file
	tsmFile, err := os.CreateTemp(filepath.Join(tempDir, "data", "12345", "autogen", "1"), "buildtsitest*"+"."+tsm1.TSMFileExtension)
	require.NoError(t, err)

	w, err := tsm1.NewTSMWriter(tsmFile)
	require.NoError(t, err)

	tsmValues := []tsm1.Value{tsm1.NewValue(0, 1.0)}
	require.NoError(t, w.Write([]byte("cpu"), tsmValues))
	require.NoError(t, w.WriteIndex())

	w.Close()

	// Create temp .wal file
	var walFile *os.File
	walFile, err = os.CreateTemp(filepath.Join(tempDir, "wal", "12345", "autogen", "1"), "buildtsitest*"+"."+tsm1.WALFileExtension)
	require.NoError(t, err)

	p1 := tsm1.NewValue(10, 1.1)
	p2 := tsm1.NewValue(1, int64(1))
	p3 := tsm1.NewValue(1, true)
	p4 := tsm1.NewValue(1, "string")
	p5 := tsm1.NewValue(5, uint64(10))

	walValues := map[string][]tsm1.Value{
		"cpu,host=A#!~#float":    {p1},
		"cpu,host=A#!~#int":      {p2},
		"cpu,host=A#!~#bool":     {p3},
		"cpu,host=A#!~#string":   {p4},
		"cpu,host=A#!~#unsigned": {p5},
	}

	writeWalFile(t, walFile, walValues)

	// Run command with appropriate parameters and expected outputs
	params := cmdParams{
		dataPath:       filepath.Join(tempDir, "data"),
		walPath:        filepath.Join(tempDir, "wal"),
		concurrency:    1,
		batchSize:      defaultBatchSize,
		maxLogFileSize: tsdb.DefaultMaxIndexLogFileSize,
		maxCacheSize:   tsdb.DefaultCacheMaxMemorySize,
		verbose:        true,
	}

	outs := cmdOuts{
		expectBuiltIndex: true,
		expectedOut:      "lvl=info",
	}

	runCommand(t, params, outs)
}

func Test_BuildTSI_Valid_Compact_Series(t *testing.T) {
	tempDir := newTempDirectory(t, "", "build-tsi")
	defer os.RemoveAll(tempDir)

	os.MkdirAll(filepath.Join(tempDir, "data", "12345", "_series"), 0777)

	// Create new series file
	sfile := tsdb.NewSeriesFile(filepath.Join(tempDir, "data", "12345", "_series"))
	err := sfile.Open()
	require.NoError(t, err)
	defer sfile.Close()

	// Generate a bunch of keys.
	var mms [][]byte
	var tagSets []models.Tags
	for i := 0; i < 1000; i++ {
		mms = append(mms, []byte("cpu"))
		tagSets = append(tagSets, models.NewTags(map[string]string{"region": fmt.Sprintf("r%d", i)}))
	}

	// Add all to the series file.
	_, err = sfile.CreateSeriesListIfNotExists(mms, tagSets)
	require.NoError(t, err)

	params := cmdParams{
		dataPath:          filepath.Join(tempDir, "data"),
		walPath:           filepath.Join(tempDir, "wal"),
		concurrency:       1,
		compactSeriesFile: true,
		batchSize:         defaultBatchSize,
		maxLogFileSize:    tsdb.DefaultMaxIndexLogFileSize,
		maxCacheSize:      tsdb.DefaultCacheMaxMemorySize,
	}

	outs := cmdOuts{
		expectCompactSeries: true,
		sfile: sfile,
	}

	runCommand(t, params, outs)
}

func initCommand(t *testing.T, params cmdParams) *cobra.Command {
	t.Helper()

	// Creates new command and sets args
	cmd := NewBuildTSICommand()

	allArgs := make([]string, 0)

	if params.dataPath != os.Getenv("HOME")+"/.influxdbv2/engine/data" {
		allArgs = append(allArgs, "--data-path", params.dataPath)
	}

	if params.walPath != os.Getenv("HOME")+"/.influxdbv2/engine/wal" {
		allArgs = append(allArgs, "--wal-path", params.walPath)
	}

	if params.bucketID != "" {
		allArgs = append(allArgs, "--bucket-id", params.bucketID)
	}

	if params.shardID != "" {
		allArgs = append(allArgs, "--shard-id", params.shardID)
	}

	if params.batchSize != 10000 {
		allArgs = append(allArgs, "--batch-size", strconv.Itoa(params.batchSize))
	}

	if params.maxLogFileSize != tsdb.DefaultMaxIndexLogFileSize {
		allArgs = append(allArgs, "--max-log-file-size", strconv.Itoa(int(params.maxLogFileSize)))
	}

	if params.maxCacheSize != tsdb.DefaultCacheMaxMemorySize {
		allArgs = append(allArgs, "--max-cache-size", strconv.Itoa(int(params.maxCacheSize)))
	}

	if params.compactSeriesFile {
		allArgs = append(allArgs, "--compact-series-file")
	}

	if params.verbose {
		allArgs = append(allArgs, "--v")
	}

	if params.concurrency != runtime.GOMAXPROCS(0) {
		allArgs = append(allArgs, "--concurrency", strconv.Itoa(params.concurrency))
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

func runCommand(t *testing.T, params cmdParams, outs cmdOuts) {
	t.Helper()

	cmd := initCommand(t, params)

	if outs.expectErr {
		require.EqualError(t, cmd.Execute(), outs.expectedOut)
		return
	}

	if outs.expectBuiltIndex {
		require.NoDirExists(t, filepath.Join(params.dataPath, "12345", "autogen", "1", "index"))
		require.NoError(t, cmd.Execute())
		require.DirExists(t, filepath.Join(params.dataPath, "12345", "autogen", "1", "index"))
	}

	if outs.expectCompactSeries {
		beforeSize, err := outs.sfile.FileSize()
		require.NoError(t, err)

		require.NoError(t, cmd.Execute())
		afterSize, err := outs.sfile.FileSize()
		require.NoError(t, err)

		require.Greater(t, beforeSize, afterSize)
	}

	if outs.expectedOut != "" || len(outs.expectedOuts) > 0 {
		// Get output
		out := getOutput(t, cmd)

		// Check output
		if outs.expectedOut != "" {
			require.Contains(t, string(out), outs.expectedOut)
		} else {
			for _, output := range outs.expectedOuts {
				require.Contains(t, string(out), output)
			}
		}
	}
}

func newTempDirectory(t *testing.T, parentDir string, dirName string) string {
	t.Helper()

	dir, err := os.MkdirTemp(parentDir, dirName)
	require.NoError(t, err)

	return dir
}

func writeWalFile(t *testing.T, file *os.File, vals map[string][]tsm1.Value) {
	t.Helper()

	e := &tsm1.WriteWALEntry{Values: vals}
	b, err := e.Encode(nil)
	require.NoError(t, err)

	w := tsm1.NewWALSegmentWriter(file)
	err = w.Write(e.Type(), snappy.Encode(nil, b))
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	err = file.Sync()
	require.NoError(t, err)
}
