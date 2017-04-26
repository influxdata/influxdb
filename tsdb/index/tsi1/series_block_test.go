package tsi1_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// Ensure series block can be unmarshaled.
func TestSeriesBlock_UnmarshalBinary(t *testing.T) {
	if _, err := CreateSeriesBlock([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure series block contains the correct set of series.
func TestSeriesBlock_Series(t *testing.T) {
	series := []Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	}
	l := MustCreateSeriesBlock(series)

	// Verify total number of series is correct.
	if n := l.SeriesCount(); n != 3 {
		t.Fatalf("unexpected series count: %d", n)
	}

	// Verify all series exist.
	for i, s := range series {
		if e := l.Series(s.Name, s.Tags); e == nil {
			t.Fatalf("series does not exist: i=%d", i)
		} else if !bytes.Equal(e.Name(), s.Name) || models.CompareTags(e.Tags(), s.Tags) != 0 {
			t.Fatalf("series element does not match: i=%d, %s (%s) != %s (%s)", i, e.Name(), e.Tags().String(), s.Name, s.Tags.String())
		} else if e.Deleted() {
			t.Fatalf("series deleted: i=%d", i)
		}
	}

	// Verify non-existent series doesn't exist.
	if e := l.Series([]byte("foo"), models.NewTags(map[string]string{"region": "north"})); e != nil {
		t.Fatalf("series should not exist: %#v", e)
	}
}

// CreateSeriesBlock returns an in-memory SeriesBlock with a list of series.
func CreateSeriesBlock(a []Series) (*tsi1.SeriesBlock, error) {
	var buf bytes.Buffer

	// Create writer and sketches. Add series.
	enc := tsi1.NewSeriesBlockEncoder(&buf)
	for i, s := range a {
		if err := enc.Encode(s.Name, s.Tags, s.Deleted); err != nil {
			return nil, fmt.Errorf("SeriesBlockWriter.Add(): i=%d, err=%s", i, err)
		}
	}

	// Close and flush.
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("SeriesBlockWriter.WriteTo(): %s", err)
	}

	// Unpack bytes into series block.
	var blk tsi1.SeriesBlock
	if err := blk.UnmarshalBinary(buf.Bytes()); err != nil {
		return nil, fmt.Errorf("SeriesBlock.UnmarshalBinary(): %s", err)
	}

	return &blk, nil
}

// MustCreateSeriesBlock calls CreateSeriesBlock(). Panic on error.
func MustCreateSeriesBlock(a []Series) *tsi1.SeriesBlock {
	l, err := CreateSeriesBlock(a)
	if err != nil {
		panic(err)
	}
	return l
}

// Series represents name/tagset pairs that are used in testing.
type Series struct {
	Name    []byte
	Tags    models.Tags
	Deleted bool
}
