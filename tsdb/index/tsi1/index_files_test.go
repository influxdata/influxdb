package tsi1_test

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// Ensure multiple index files can be compacted together.
func TestIndexFiles_WriteTo(t *testing.T) {
	sfile := MustOpenSeriesFile()
	defer sfile.Close()

	// Write first file.
	f0, err := CreateIndexFile(sfile.SeriesFile, []Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Write second file.
	f1, err := CreateIndexFile(sfile.SeriesFile, []Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("disk"), Tags: models.NewTags(map[string]string{"region": "east"})},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Compact the two together and write out to a buffer.
	var buf bytes.Buffer
	a := tsi1.IndexFiles{f0, f1}
	if n, err := a.CompactTo(&buf, sfile.SeriesFile, M, K, nil); err != nil {
		t.Fatal(err)
	} else if n == 0 {
		t.Fatal("expected data written")
	}

	// Unmarshal buffer into a new index file.
	f := tsi1.NewIndexFile(sfile.SeriesFile)
	if err := f.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	// Verify data in compacted file.
	if e := f.TagValueElem([]byte("cpu"), []byte("region"), []byte("west")); e == nil {
		t.Fatal("expected element")
	} else if n := e.(*tsi1.TagBlockValueElem).SeriesN(); n != 1 {
		t.Fatalf("unexpected series count: %d", n)
	}
}
