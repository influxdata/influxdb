package verify_seriesfile

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestVerifies_BasicCobra(t *testing.T) {
	test := NewTest(t)
	defer os.RemoveAll(test.Path)

	verify := NewVerifySeriesfileCommand()
	verify.SetArgs([]string{"--data-path", test.Path})
	require.NoError(t, verify.Execute())
}

func TestVerifies_Valid(t *testing.T) {
	test := NewTest(t)
	defer os.RemoveAll(test.Path)

	verify := newVerify()
	verify.Logger = zaptest.NewLogger(t)

	passed, err := verify.verifySeriesFile(test.Path)
	require.NoError(t, err)
	require.True(t, passed)
}

func TestVerifies_Invalid(t *testing.T) {
	test := NewTest(t)
	defer os.RemoveAll(test.Path)

	require.NoError(t, filepath.WalkDir(test.Path, func(path string, entry os.DirEntry, err error) error {
		require.NoError(t, err)

		if entry.IsDir() {
			return nil
		}

		test.Backup(path)
		defer test.Restore(path)

		fh, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		defer fh.Close()

		_, err = fh.WriteAt([]byte("foobar"), 0)
		require.NoError(t, err)
		require.NoError(t, fh.Close())

		verify := newVerify()
		verify.Logger = zaptest.NewLogger(t)

		passed, err := verify.verifySeriesFile(test.Path)
		require.NoError(t, err)
		require.False(t, passed)

		return nil
	}))
}

type Test struct {
	*testing.T
	Path string
}

func NewTest(t *testing.T) *Test {
	t.Helper()

	dir, err := os.MkdirTemp("", "verify-seriesfile-")
	require.NoError(t, err)

	// create a series file in the directory
	err = func() error {
		seriesFile := tsdb.NewSeriesFile(dir)
		if err := seriesFile.Open(); err != nil {
			return err
		}
		defer seriesFile.Close()
		seriesFile.EnableCompactions()

		const (
			compactionThreshold = 100
			numSeries           = 2 * tsdb.SeriesFilePartitionN * compactionThreshold
		)

		for _, partition := range seriesFile.Partitions() {
			partition.CompactThreshold = compactionThreshold
		}

		var names [][]byte
		var tagsSlice []models.Tags

		for i := 0; i < numSeries; i++ {
			names = append(names, []byte(fmt.Sprintf("series%d", i)))
			tagsSlice = append(tagsSlice, nil)
		}

		ids, err := seriesFile.CreateSeriesListIfNotExists(names, tagsSlice)
		if err != nil {
			return err
		}

		// delete one series
		if err := seriesFile.DeleteSeriesID(ids[0]); err != nil {
			return err
		}

		// wait for compaction to make sure we detect issues with the index
		partitions := seriesFile.Partitions()
	wait:
		for _, partition := range partitions {
			if partition.Compacting() {
				time.Sleep(100 * time.Millisecond)
				goto wait
			}
		}

		return seriesFile.Close()
	}()
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	return &Test{
		T:    t,
		Path: dir,
	}
}

// Backup makes a copy of the path for a later Restore.
func (t *Test) Backup(path string) {
	in, err := os.Open(path)
	require.NoError(t.T, err)
	defer in.Close()

	out, err := os.Create(path + ".backup")
	require.NoError(t.T, err)
	defer out.Close()

	_, err = io.Copy(out, in)
	require.NoError(t.T, err)
}

// Restore restores the file at the path to the time when Backup was called last.
func (t *Test) Restore(path string) {
	require.NoError(t.T, os.Rename(path+".backup", path))
}
