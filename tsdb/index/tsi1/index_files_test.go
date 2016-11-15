package tsi1_test

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// Ensure multiple index files can be compacted together.
func TestIndexFiles_WriteTo(t *testing.T) {
	// Write first file.
	f0, err := CreateIndexFile([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Write second file.
	f1, err := CreateIndexFile([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("disk"), Tags: models.NewTags(map[string]string{"region": "east"})},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Compact the two together and write out to a buffer.
	var buf bytes.Buffer
	a := tsi1.IndexFiles{f0, f1}
	if n, err := a.WriteTo(&buf); err != nil {
		t.Fatal(err)
	} else if n == 0 {
		t.Fatal("expected data written")
	}

	// Unmarshal buffer into a new index file.
	var f tsi1.IndexFile
	if err := f.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	// Verify data in compacted file.
	if e, err := f.TagValueElem([]byte("cpu"), []byte("region"), []byte("west")); err != nil {
		t.Fatal(err)
	} else if e.SeriesN() != 1 {
		t.Fatalf("unexpected series count: %d", e.SeriesN())
	}
}
