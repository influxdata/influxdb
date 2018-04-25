package verify_seriesfile

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

func TestValidates_Valid(t *testing.T) {
	test := NewTest(t)
	defer test.Close()

	test.CreateSeriesFile()

	cmd := NewTestCommand()
	passed, err := cmd.runFile(test.Dir)
	test.AssertNoError(err)
	test.Assert(passed)
}

func TestValidates_Invalid(t *testing.T) {
	test := NewTest(t)
	defer test.Close()

	test.CreateSeriesFile()

	// mutate all the files in the first partition and make sure it fails. the
	// reason we don't do every partition is to avoid quadratic time because
	// the implementation checks the partitions in order.

	test.AssertNoError(filepath.Walk(filepath.Join(test.Dir, "00"),
		func(path string, info os.FileInfo, err error) error {
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

			cmd := NewTestCommand()
			passed, err := cmd.runFile(test.Dir)
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
	Dir string
}

func NewTest(t *testing.T) *Test {
	dir, err := ioutil.TempDir("", "verify-seriesfile-")
	if err != nil {
		t.Fatal(err)
	}

	return &Test{
		T:   t,
		Dir: dir,
	}
}

func (t *Test) Close() {
	os.RemoveAll(t.Dir)
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

func (t *Test) CreateSeriesFile() {
	seriesFile := tsdb.NewSeriesFile(t.Dir)
	t.AssertNoError(seriesFile.Open())
	defer seriesFile.Close()

	seriesFile.EnableCompactions()

	var names [][]byte
	var tagsSlice []models.Tags
	for i := 0; i < 2*tsdb.SeriesFilePartitionN*tsdb.DefaultSeriesPartitionCompactThreshold; i++ {
		names = append(names, []byte(fmt.Sprintf("series%d", i)))
		tagsSlice = append(tagsSlice, nil)
	}

	_, err := seriesFile.CreateSeriesListIfNotExists(names, tagsSlice, nil)
	t.AssertNoError(err)

	// wait for compaction to make sure we detect issues with the index
	partitions := seriesFile.Partitions()
wait:
	for _, partition := range partitions {
		if partition.Compacting() {
			time.Sleep(100 * time.Millisecond)
			goto wait
		}
	}

	t.AssertNoError(seriesFile.Close())
}

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

func (t *Test) Restore(path string) {
	t.AssertNoError(os.Rename(path+".backup", path))
}

type TestCommand struct{ Command }

func NewTestCommand() *TestCommand {
	return &TestCommand{Command{
		Stderr: new(bytes.Buffer),
		Stdout: new(bytes.Buffer),
	}}
}

func (t *TestCommand) StdoutString() string {
	return t.Stdout.(*bytes.Buffer).String()
}

func (t *TestCommand) StderrString() string {
	return t.Stderr.(*bytes.Buffer).String()
}
