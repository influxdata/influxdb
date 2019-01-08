package tsdb_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

func toTypedSeriesID(id uint64) tsdb.SeriesIDTyped {
	return tsdb.NewSeriesID(id).WithType(models.Empty)
}

func TestSeriesIndex_Count(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	idx := tsdb.NewSeriesIndex(filepath.Join(dir, "index"))
	if err := idx.Open(); err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	key0 := tsdb.AppendSeriesKey(nil, []byte("m0"), nil)
	idx.Insert(key0, toTypedSeriesID(1), 10)
	key1 := tsdb.AppendSeriesKey(nil, []byte("m1"), nil)
	idx.Insert(key1, toTypedSeriesID(2), 20)

	if n := idx.Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	}
}

func TestSeriesIndex_Delete(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	idx := tsdb.NewSeriesIndex(filepath.Join(dir, "index"))
	if err := idx.Open(); err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	key0 := tsdb.AppendSeriesKey(nil, []byte("m0"), nil)
	idx.Insert(key0, toTypedSeriesID(1), 10)
	key1 := tsdb.AppendSeriesKey(nil, []byte("m1"), nil)
	idx.Insert(key1, toTypedSeriesID(2), 20)
	idx.Delete(tsdb.NewSeriesID(1))

	if !idx.IsDeleted(tsdb.NewSeriesID(1)) {
		t.Fatal("expected deletion")
	} else if idx.IsDeleted(tsdb.NewSeriesID(2)) {
		t.Fatal("expected series to exist")
	}
}

func TestSeriesIndex_FindIDBySeriesKey(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	idx := tsdb.NewSeriesIndex(filepath.Join(dir, "index"))
	if err := idx.Open(); err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	key0 := tsdb.AppendSeriesKey(nil, []byte("m0"), nil)
	idx.Insert(key0, toTypedSeriesID(1), 10)
	key1 := tsdb.AppendSeriesKey(nil, []byte("m1"), nil)
	idx.Insert(key1, toTypedSeriesID(2), 20)
	badKey := tsdb.AppendSeriesKey(nil, []byte("not_found"), nil)

	if id := idx.FindIDBySeriesKey(nil, key0); id != toTypedSeriesID(1) {
		t.Fatalf("unexpected id(0): %d", id)
	} else if id := idx.FindIDBySeriesKey(nil, key1); id != toTypedSeriesID(2) {
		t.Fatalf("unexpected id(1): %d", id)
	} else if id := idx.FindIDBySeriesKey(nil, badKey); !id.IsZero() {
		t.Fatalf("unexpected id(2): %d", id)
	}

	if id := idx.FindIDByNameTags(nil, []byte("m0"), nil, nil); id != toTypedSeriesID(1) {
		t.Fatalf("unexpected id(0): %d", id)
	} else if id := idx.FindIDByNameTags(nil, []byte("m1"), nil, nil); id != toTypedSeriesID(2) {
		t.Fatalf("unexpected id(1): %d", id)
	} else if id := idx.FindIDByNameTags(nil, []byte("not_found"), nil, nil); !id.IsZero() {
		t.Fatalf("unexpected id(2): %d", id)
	}
}

func TestSeriesIndex_FindOffsetByID(t *testing.T) {
	dir, cleanup := MustTempDir()
	defer cleanup()

	idx := tsdb.NewSeriesIndex(filepath.Join(dir, "index"))
	if err := idx.Open(); err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	idx.Insert(tsdb.AppendSeriesKey(nil, []byte("m0"), nil), toTypedSeriesID(1), 10)
	idx.Insert(tsdb.AppendSeriesKey(nil, []byte("m1"), nil), toTypedSeriesID(2), 20)

	if offset := idx.FindOffsetByID(tsdb.NewSeriesID(1)); offset != 10 {
		t.Fatalf("unexpected offset(0): %d", offset)
	} else if offset := idx.FindOffsetByID(tsdb.NewSeriesID(2)); offset != 20 {
		t.Fatalf("unexpected offset(1): %d", offset)
	} else if offset := idx.FindOffsetByID(tsdb.NewSeriesID(3)); offset != 0 {
		t.Fatalf("unexpected offset(2): %d", offset)
	}
}

func TestSeriesIndexHeader(t *testing.T) {
	// Verify header initializes correctly.
	hdr := tsdb.NewSeriesIndexHeader()
	if hdr.Version != tsdb.SeriesIndexVersion {
		t.Fatalf("unexpected version: %d", hdr.Version)
	}
	hdr.MaxSeriesID = tsdb.NewSeriesID(10)
	hdr.MaxOffset = 20
	hdr.Count = 30
	hdr.Capacity = 40
	hdr.KeyIDMap.Offset, hdr.KeyIDMap.Size = 50, 60
	hdr.IDOffsetMap.Offset, hdr.IDOffsetMap.Size = 70, 80

	// Marshal/unmarshal.
	var buf bytes.Buffer
	if _, err := hdr.WriteTo(&buf); err != nil {
		t.Fatal(err)
	} else if other, err := tsdb.ReadSeriesIndexHeader(buf.Bytes()); err != nil {
		t.Fatal(err)
	} else if diff := cmp.Diff(hdr, other); diff != "" {
		t.Fatal(diff)
	}
}
