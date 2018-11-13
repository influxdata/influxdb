package seriesfile_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/cmd/influx_inspect/verify/seriesfile"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

func TestVerifies_Valid(t *testing.T) {
	test := NewTest(t)
	defer test.Close()

	verify := seriesfile.NewVerify()
	if testing.Verbose() {
		verify.Logger, _ = zap.NewDevelopment()
	}
	passed, err := verify.VerifySeriesFile(test.Path)
	test.AssertNoError(err)
	test.Assert(passed)
}

func TestVerifies_Invalid(t *testing.T) {
	test := NewTest(t)
	defer test.Close()

	test.AssertNoError(filepath.Walk(test.Path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		test.Backup(path)
		defer test.Restore(path)

		fh, err := os.OpenFile(path, os.O_RDWR, 0)
		test.AssertNoError(err)
		defer fh.Close()

		_, err = fh.WriteAt([]byte("BOGUS"), 0)
		test.AssertNoError(err)
		test.AssertNoError(fh.Close())

		passed, err := seriesfile.NewVerify().VerifySeriesFile(test.Path)
		test.AssertNoError(err)
		test.Assert(!passed)

		return nil
	}))
}

//
// helpers
//

type Test struct {
	*testing.T
	Path string
}

func NewTest(t *testing.T) *Test {
	t.Helper()

	dir, err := ioutil.TempDir("", "verify-seriesfile-")
	if err != nil {
		t.Fatal(err)
	}

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

		_, err := seriesFile.CreateSeriesListIfNotExists(names, tagsSlice)
		if err != nil {
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

func (t *Test) Close() {
	os.RemoveAll(t.Path)
}

func (t *Test) AssertNoError(err error) {
	t.Helper()
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
}

func (t *Test) Assert(x bool) {
	t.Helper()
	if !x {
		t.Fatal("unexpected condition")
	}
}

// Backup makes a copy of the path for a later Restore.
func (t *Test) Backup(path string) {
	in, err := os.Open(path)
	t.AssertNoError(err)
	defer in.Close()

	out, err := os.Create(path + ".backup")
	t.AssertNoError(err)
	defer out.Close()

	_, err = io.Copy(out, in)
	t.AssertNoError(err)
}

// Restore restores the file at the path to the time when Backup was called last.
func (t *Test) Restore(path string) {
	t.AssertNoError(os.Rename(path+".backup", path))
}
