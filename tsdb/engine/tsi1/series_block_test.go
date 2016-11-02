package tsi1_test

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsi1"
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

// Ensure series block contains the correct term count and term encoding.
func TestSeriesBlock_Terms(t *testing.T) {
	l := MustCreateSeriesBlock([]Series{
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: []byte("cpu"), Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: []byte("mem"), Tags: models.NewTags(map[string]string{"region": "east"})},
	})

	// Verify term count is correct.
	if n := l.TermCount(); n != 5 {
		t.Fatalf("unexpected term count: %d", n)
	}

	// Encode & decode all terms.
	for _, term := range []string{"cpu", "mem", "region", "east", "west"} {
		// Encode term.
		offset := l.EncodeTerm([]byte(term))
		if offset == 0 {
			t.Errorf("term not found: %s", term)
			continue
		}

		// Decode term offset.
		if v := l.DecodeTerm(offset); !bytes.Equal([]byte(term), v) {
			t.Errorf("decode mismatch: got=%s, exp=%s", term, v)
		}
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

	// Ensure series can encode & decode correctly.
	var name []byte
	var tags models.Tags
	for _, series := range series {
		l.DecodeSeries(l.EncodeSeries(series.Name, series.Tags), &name, &tags)
		if !bytes.Equal(name, series.Name) || !reflect.DeepEqual(tags, series.Tags) {
			t.Fatalf("encoding mismatch: got=%s/%#v, exp=%s/%#v", name, tags, series.Name, series.Tags)
		}
	}

	// Verify all series exist.
	for i, s := range series {
		if offset, deleted := l.SeriesOffset(l.EncodeSeries(s.Name, s.Tags)); offset == 0 {
			t.Fatalf("series does not exist: i=%d", i)
		} else if deleted {
			t.Fatalf("series deleted: i=%d", i)
		}
	}

	// Verify non-existent series doesn't exist.
	if offset, deleted := l.SeriesOffset(l.EncodeSeries([]byte("foo"), models.NewTags(map[string]string{"region": "north"}))); offset != 0 {
		t.Fatalf("series should not exist: offset=%d", offset)
	} else if deleted {
		t.Fatalf("series should not be deleted")
	}
}

// CreateSeriesBlock returns an in-memory SeriesBlock with a list of series.
func CreateSeriesBlock(a []Series) (*tsi1.SeriesBlock, error) {
	// Create writer and add series.
	w := tsi1.NewSeriesBlockWriter()
	for i, s := range a {
		if err := w.Add(s.Name, s.Tags); err != nil {
			return nil, fmt.Errorf("SeriesBlockWriter.Add(): i=%d, err=%s", i, err)
		}
	}

	// Write to buffer.
	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
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
	Name []byte
	Tags models.Tags
}
