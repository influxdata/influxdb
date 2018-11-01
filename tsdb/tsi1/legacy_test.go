package tsi1

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/platform/tsdb"
)

func TestLegacyOpen(t *testing.T) {
	dir, err := ioutil.TempDir("", "tsi1-")
	if err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(dir)

	sfile := tsdb.NewSeriesFile(dir)
	if err := sfile.Open(); err != nil {
		t.Fatal(err)
	}
	defer sfile.Close()

	index := NewIndex(sfile, "db", NewConfig(), WithPath("testdata/index-file-index"))
	if err := index.Open(); err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	// check that we can read all the measurements
	err = index.ForEachMeasurementName(func(name []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
}
