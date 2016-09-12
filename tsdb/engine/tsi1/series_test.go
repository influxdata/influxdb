package tsi1_test

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsi1"
)

// Ensure series list can be unmarshaled.
func TestSeriesList_UnmarshalBinary(t *testing.T) {
	if _, err := CreateSeriesList([]Series{
		{Name: "cpu", Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: "cpu", Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: "mem", Tags: models.NewTags(map[string]string{"region": "east"})},
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure series list returns an error if the buffer is too small.
func TestSeriesList_UnmarshalBinary_ErrShortBuffer(t *testing.T) {
	var l tsi1.SeriesList
	if err := l.UnmarshalBinary(make([]byte, 4)); err != io.ErrShortBuffer {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure series list contains the correct term count and term encoding.
func TestSeriesList_Terms(t *testing.T) {
	l := MustCreateSeriesList([]Series{
		{Name: "cpu", Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: "cpu", Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: "mem", Tags: models.NewTags(map[string]string{"region": "east"})},
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

// Ensure series list contains the correct set of series.
func TestSeriesList_Series(t *testing.T) {
	series := []Series{
		{Name: "cpu", Tags: models.NewTags(map[string]string{"region": "east"})},
		{Name: "cpu", Tags: models.NewTags(map[string]string{"region": "west"})},
		{Name: "mem", Tags: models.NewTags(map[string]string{"region": "east"})},
	}
	l := MustCreateSeriesList(series)

	// Verify total number of series is correct.
	if n := l.SeriesCount(); n != 3 {
		t.Fatalf("unexpected series count: %d", n)
	}

	// Ensure series can encode & decode correctly.
	for _, series := range series {
		name, tags := l.DecodeSeries(l.EncodeSeries(series.Name, series.Tags))
		if name != series.Name || !reflect.DeepEqual(tags, series.Tags) {
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
	if offset, deleted := l.SeriesOffset(l.EncodeSeries("foo", models.NewTags(map[string]string{"region": "north"}))); offset != 0 {
		t.Fatalf("series should not exist: offset=%d", offset)
	} else if deleted {
		t.Fatalf("series should not be deleted")
	}
}

// CreateSeriesList returns an in-memory SeriesList with a list of series.
func CreateSeriesList(a []Series) (*tsi1.SeriesList, error) {
	// Create writer and add series.
	w := tsi1.NewSeriesListWriter()
	for i, s := range a {
		if err := w.Add(s.Name, s.Tags); err != nil {
			return nil, fmt.Errorf("SeriesListWriter.Add(): i=%d, err=%s", i, err)
		}
	}

	// Write to buffer.
	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		return nil, fmt.Errorf("SeriesListWriter.WriteTo(): %s", err)
	}

	// Unpack bytes into series list.
	var l tsi1.SeriesList
	if err := l.UnmarshalBinary(buf.Bytes()); err != nil {
		return nil, fmt.Errorf("SeriesList.UnmarshalBinary(): %s", err)
	}

	return &l, nil
}

// MustCreateSeriesList calls CreateSeriesList(). Panic on error.
func MustCreateSeriesList(a []Series) *tsi1.SeriesList {
	l, err := CreateSeriesList(a)
	if err != nil {
		panic(err)
	}
	return l
}

// Series represents name/tagset pairs that are used in testing.
type Series struct {
	Name string
	Tags models.Tags
}
