package report_tsi

import (
	"bytes"
	"fmt"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/influxdata/influxdb/v2/tsdb/index/tsi1"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
)

type cmdParams struct {
	bucketId    	 string
	concurrency 	 int
	enginePath    	 string
	topN        	 int
	expectedOut      string
	expectedOuts     []string
	expectErr        bool
	expectExactEqual bool
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
		bucketId: "12345",
		expectErr:   true,
		expectedOut: "open 12345/autogen: no such file or directory",
	}
	runCommand(t, params)
}

func Test_ReportTSI_Bucket_Contains_No_Shards(t *testing.T) {
	path := newTempDirectories(t, false)
	defer os.RemoveAll(path)
	params := cmdParams{
		bucketId: "12345",
		enginePath: path,
		expectedOut: "No shards under "+filepath.Join(path, "12345", "autogen"),
	}
	runCommand(t, params)
}

func Test_ReportTSI_Valid(t *testing.T) {
	path := newTempDirectories(t, true)
	fmt.Printf("root temp path: %s\n", path)
	defer os.RemoveAll(path)

	makeTSIFile(t, tsiParams{
		seriesPath: filepath.Join(path, "12345", "_series"),
		indexPath: filepath.Join(path, "12345", "autogen", "1", "index", "0"),
	})

	params := cmdParams{
		bucketId: "12345",
		enginePath: path,
		expectedOut: "valid output",
	}
	runCommand(t, params)
}

func newTempDirectories(t *testing.T, withShards bool) string {
	t.Helper()

	dataDir, err := os.MkdirTemp("", "reporttsi")
	require.NoError(t, err)

	err = os.MkdirAll(filepath.Join(dataDir, "12345", "autogen"),0777)
	require.NoError(t, err)

	if withShards {
		// Create shard and index directory within it, with one partition
		err = os.MkdirAll(filepath.Join(dataDir, "12345", "autogen", "1", "index", "0"),0777)
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
		allArgs = append(allArgs, "--c", strconv.Itoa(params.concurrency))
	}

	if params.enginePath != os.Getenv("HOME")+"/.influxdbv2/engine/data" {
		allArgs = append(allArgs, "--path", params.enginePath)
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
	if params.expectExactEqual {
		require.Equal(t, string(out), params.expectedOut)
		return
	}

	if params.expectedOut != "" {
		require.Contains(t, string(out), params.expectedOut)
	} else {
		for _, output := range params.expectedOuts {
			require.Contains(t, string(out), output)
		}
	}
}

type tsiParams struct {
	badEntry bool
	seriesPath string
	indexPath string
}

func makeTSIFile(t *testing.T, params tsiParams) string {
	t.Helper()

	sfile := tsdb.NewSeriesFile(params.seriesPath)
	require.NoError(t, sfile.Open())
	defer sfile.Close()

	f, err := createIndexFile(t, params.indexPath, sfile, []series{
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
	})
	require.NoError(t, err)

	if params.badEntry {
		require.NoError(t, os.WriteFile(f.Path(), []byte("foobar"), 0666))
	}
	return f.Path()
}

// series represents name/tagset pairs that are used in testing.
type series struct {
	Name    []byte
	Tags    models.Tags
	Deleted bool
}

// createIndexFile creates an index file with a given set of series.
func createIndexFile(t *testing.T, indexPath string, sfile *tsdb.SeriesFile, series []series) (*tsi1.IndexFile, error) {
	t.Helper()

	lf, err := createLogFile(t, indexPath, sfile, series)
	require.NoError(t, err)

	// Write index file to buffer.
	var buf bytes.Buffer
	_, err = lf.CompactTo(&buf, 4096, 6, nil)

	require.NoError(t, err)

	// Load index file from buffer.
	f := tsi1.NewIndexFile(sfile)
	err = f.Open()
	require.NoError(t, err)

	defer f.Close()
	err = f.UnmarshalBinary(buf.Bytes())
	require.NoError(t, err)
	return f, nil
}

// createLogFile creates a new temporary log file and adds a list of series.
func createLogFile(t *testing.T, indexPath string, sfile *tsdb.SeriesFile, series []series) (*tsi1.LogFile, error) {
	t.Helper()

	f := newLogFile(t, indexPath, sfile)
	require.NoError(t, f.Open())
	seriesSet := tsdb.NewSeriesIDSet()
	for _, serie := range series {
		_, err := f.AddSeriesList(seriesSet, [][]byte{serie.Name}, []models.Tags{serie.Tags})
		require.NoError(t, err)
	}
	return f, nil
}

func newLogFile(t *testing.T, indexPath string, sfile *tsdb.SeriesFile) *tsi1.LogFile {
	t.Helper()

	return tsi1.NewLogFile(sfile, filepath.Join(indexPath, "L0-00000001.tsl"))
}
