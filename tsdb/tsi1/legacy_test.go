package tsi1

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/influxdata/influxdb/v2/tsdb/seriesfile"
)

func TestLegacyOpen(t *testing.T) {
	dir, err := ioutil.TempDir("", "tsi1-")
	if err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(dir)

	sfile := seriesfile.NewSeriesFile(dir)
	if err := sfile.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer sfile.Close()

	index := NewIndex(sfile, NewConfig(), WithPath("testdata/index-file-index"))
	if err := index.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	// check that we can read all the measurements
	err = index.ForEachMeasurementName(func(name []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
}
