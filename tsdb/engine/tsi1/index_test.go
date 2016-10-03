package tsi1_test

import (
	"bytes"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsi1"
)

// Ensure index can be built and opened.
func TestIndex(t *testing.T) {
	series := []Series{
		{Name: "cpu", Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: "cpu", Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: "mem", Tags: models.NewTags(map[string]string{"region": "east"})},
	}

	// Add series to the writer.
	iw := tsi1.NewIndexWriter()
	for _, serie := range series {
		iw.Add(serie.Name, serie.Tags)
	}

	// Write index to buffer.
	var buf bytes.Buffer
	if _, err := iw.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}

	// Load index from buffer.
	var idx tsi1.Index
	if err := idx.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
}
