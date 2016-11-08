package tsi1_test

import (
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsi1"
)

// Ensure index can return a single measurement by name.
func TestIndex_Measurement(t *testing.T) {
	// Build an index file.
	f, err := CreateIndexFile([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create an index from the single file.
	var idx tsi1.Index
	idx.SetIndexFiles(f)

	// Verify measurement is correct.
	if mm, err := idx.Measurement([]byte("cpu")); err != nil {
		t.Fatal(err)
	} else if mm == nil {
		t.Fatal("expected measurement")
	}

	// Verify non-existent measurement doesn't exist.
	if mm, err := idx.Measurement([]byte("no_such_measurement")); err != nil {
		t.Fatal(err)
	} else if mm != nil {
		t.Fatal("expected nil measurement")
	}
}

// Ensure index can return a list of all measurements.
func TestIndex_Measurements(t *testing.T) {
	// Build an index file.
	f, err := CreateIndexFile([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create an index from the single file.
	var idx tsi1.Index
	idx.SetIndexFiles(f)

	// Retrieve measurements and verify.
	if mms, err := idx.Measurements(); err != nil {
		t.Fatal(err)
	} else if len(mms) != 2 {
		t.Fatalf("expected measurement count: %d", len(mms))
	} else if mms[0].Name != "cpu" {
		t.Fatalf("unexpected measurement(0): %s", mms[0].Name)
	} else if mms[1].Name != "mem" {
		t.Fatalf("unexpected measurement(1): %s", mms[1].Name)
	}
}
