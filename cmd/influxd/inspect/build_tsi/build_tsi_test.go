package build_tsi

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/engine/tsm1"
	"github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type cmdParams struct {
	dataPath          string
	walPath           string
	bucketID          string
	shardID           string
	batchSize         int
	maxLogFileSize    int64
	maxCacheSize      uint64
	compactSeriesFile bool
	concurrency       int
	verbose           bool
}

type cmdOuts struct {
	expectedOut         string
	expectErr           bool
	expectBuiltIndex    bool
	expectCompactSeries bool
	sfilePath           string
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

	// Create a temp .tsm file
	tsmValues := []tsm1.Value{tsm1.NewValue(0, 1.0)}
	newTempTsmFile(t, filepath.Join(tempDir, "data", "12345", "autogen", "1"), tsmValues)

	// Create a temp .wal file
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

	newTempWalFile(t, filepath.Join(tempDir, "wal", "12345", "autogen", "1"), walValues)

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

func Test_BuildTSI_Valid_Batch_Size_Exceeded(t *testing.T) {
	tempDir := newTempDirectory(t, "", "build-tsi")
	defer os.RemoveAll(tempDir)

	os.MkdirAll(filepath.Join(tempDir, "data", "12345", "autogen", "1"), 0777)
	os.MkdirAll(filepath.Join(tempDir, "wal", "12345", "autogen", "1"), 0777)

	// Create a temp .tsm file
	tsmValues := []tsm1.Value{tsm1.NewValue(0, 1.0)}
	newTempTsmFile(t, filepath.Join(tempDir, "data", "12345", "autogen", "1"), tsmValues)

	// Create a temp .wal file
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

	newTempWalFile(t, filepath.Join(tempDir, "wal", "12345", "autogen", "1"), walValues)

	params := cmdParams{
		dataPath:       filepath.Join(tempDir, "data"),
		walPath:        filepath.Join(tempDir, "wal"),
		concurrency:    1,
		batchSize:      1,
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

	// Create a temp .tsm file
	tsmValues := []tsm1.Value{tsm1.NewValue(0, 1.0)}
	newTempTsmFile(t, filepath.Join(tempDir, "data", "12345", "autogen", "1"), tsmValues)

	// Create temp .wal file
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

	newTempWalFile(t, filepath.Join(tempDir, "wal", "12345", "autogen", "1"), walValues)

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

func Test_BuildTSI_Invalid_Compact_Series_Specific_Shard(t *testing.T) {
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

func Test_BuildTSI_Valid_Compact_Series(t *testing.T) {
	tempDir := newTempDirectory(t, "", "build-tsi")
	defer os.RemoveAll(tempDir)

	os.MkdirAll(filepath.Join(tempDir, "data", "12345", "_series"), 0777)

	// Create new series file
	sfile := tsdb.NewSeriesFile(filepath.Join(tempDir, "data", "12345", "_series"))
	require.NoError(t, sfile.Open())
	defer sfile.Close()

	// Generate a bunch of keys.
	var mms [][]byte
	var tagSets []models.Tags
	for i := 0; i < 1000; i++ {
		mms = append(mms, []byte("cpu"))
		tagSets = append(tagSets, models.NewTags(map[string]string{"region": fmt.Sprintf("r%d", i)}))
	}

	// Add all to the series file.
	_, err := sfile.CreateSeriesListIfNotExists(mms, tagSets)
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
		sfilePath:           sfile.Path(),
	}

	require.NoError(t, sfile.Close())
	runCommand(t, params, outs)
}

func initCommand(t *testing.T, params cmdParams) *cobra.Command {
	t.Helper()

	// Create new command
	cmd := NewBuildTSICommand()

	// Set args
	allArgs := make([]string, 0)

	if params.dataPath != filepath.Join(os.Getenv("HOME"), ".influxdbv2", "engine", "data") {
		allArgs = append(allArgs, "--data-path", params.dataPath)
	}
	if params.walPath != filepath.Join(os.Getenv("HOME"), ".influxdbv2", "engine", "wal") {
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
		allArgs = append(allArgs, "-v")
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

		// Check that a valid index directory is present after executing the command
		isIndex, err := tsi1.IsIndexDir(filepath.Join(params.dataPath, "12345", "autogen", "1", "index"))
		require.NoError(t, err)
		require.True(t, isIndex)

		// Check manifest files, at least one index file should be listed in each
		for i := 0; i < 8; i++ {
			currentPartition := strconv.Itoa(i)
			manifest, _, err := tsi1.ReadManifestFile(filepath.Join(params.dataPath, "12345", "autogen", "1", "index", currentPartition, "MANIFEST"))
			require.NoError(t, err)
			require.NotZero(t, len(manifest.Files))
		}
	}

	if outs.expectCompactSeries {
		sfile := tsdb.NewSeriesFile(outs.sfilePath)
		require.NoError(t, sfile.Open())
		defer sfile.Close()

		// Get size of all partitions before series compaction
		beforeSize, err := sfile.FileSize()
		require.NoError(t, err)
		require.NoError(t, sfile.Close())

		// Run command with series compaction option chosen
		require.NoError(t, cmd.Execute())

		// Check if series directory exists
		require.DirExists(t, filepath.Join(params.dataPath, "12345", "_series"))

		// Get size of all partitions after series compaction
		require.NoError(t, sfile.Open())
		afterSize, err := sfile.FileSize()
		require.NoError(t, err)

		// Check that collective size of all series partitions has decreased after compaction
		require.Greater(t, beforeSize, afterSize)
	}

	if outs.expectedOut != "" {
		// Get output
		out := getOutput(t, cmd)

		// Check output
		if outs.expectedOut != "" {
			require.Contains(t, string(out), outs.expectedOut)
		}
	}
}

func newTempDirectory(t *testing.T, parentDir string, dirName string) string {
	t.Helper()

	dir, err := os.MkdirTemp(parentDir, dirName)
	require.NoError(t, err)

	return dir
}

func newTempTsmFile(t *testing.T, path string, values []tsm1.Value) {
	t.Helper()

	tsmFile, err := os.CreateTemp(path, "buildtsitest*"+"."+tsm1.TSMFileExtension)
	require.NoError(t, err)

	w, err := tsm1.NewTSMWriter(tsmFile)
	require.NoError(t, err)

	require.NoError(t, w.Write([]byte("cpu"), values))
	require.NoError(t, w.WriteIndex())

	w.Close()
}

func newTempWalFile(t *testing.T, path string, values map[string][]tsm1.Value) {
	t.Helper()

	walFile, err := os.CreateTemp(path, "buildtsitest*"+"."+tsm1.WALFileExtension)
	require.NoError(t, err)

	e := &tsm1.WriteWALEntry{Values: values}
	b, err := e.Encode(nil)
	require.NoError(t, err)

	w := tsm1.NewWALSegmentWriter(walFile)
	err = w.Write(e.Type(), snappy.Encode(nil, b))
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	err = walFile.Sync()
	require.NoError(t, err)
}
