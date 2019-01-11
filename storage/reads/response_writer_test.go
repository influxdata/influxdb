package reads_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"google.golang.org/grpc/metadata"
)

func TestResponseWriter_WriteResultSet_Stats(t *testing.T) {
	scannedValues := 37
	scannedBytes := 41

	var gotTrailer metadata.MD = nil

	stream := mock.NewResponseStream()
	stream.SetTrailerFunc = func(trailer metadata.MD) {
		if gotTrailer != nil {
			t.Error("trailer expected to be set once, but SetTrailer was called more than once")
		} else {
			gotTrailer = trailer
		}
	}

	rs := mock.NewResultSet()
	rs.StatsFunc = func() cursors.CursorStats {
		return cursors.CursorStats{
			ScannedValues: scannedValues,
			ScannedBytes:  scannedBytes,
		}
	}
	nextHasBeenCalledOnce := false
	rs.NextFunc = func() bool { // Returns true exactly once
		if !nextHasBeenCalledOnce {
			nextHasBeenCalledOnce = true
			return true
		}
		return false
	}
	cursorHasBeenCalledOnce := false
	rs.CursorFunc = func() cursors.Cursor {
		if !cursorHasBeenCalledOnce {
			cursorHasBeenCalledOnce = true
			return mock.NewIntegerArrayCursor()
		}
		return nil
	}

	// This is what we're testing.
	rw := reads.NewResponseWriter(stream, 0)
	err := rw.WriteResultSet(rs)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(gotTrailer.Get("scanned-values"), []string{fmt.Sprint(scannedValues)}) {
		t.Errorf("expected scanned-values '%v' but got '%v'", []string{fmt.Sprint(scannedValues)}, gotTrailer.Get("scanned-values"))
	}
	if !reflect.DeepEqual(gotTrailer.Get("scanned-bytes"), []string{fmt.Sprint(scannedBytes)}) {
		t.Errorf("expected scanned-bytes '%v' but got '%v'", []string{fmt.Sprint(scannedBytes)}, gotTrailer.Get("scanned-bytes"))
	}
}

func TestResponseWriter_WriteGroupResultSet_Stats(t *testing.T) {
	scannedValues := 37
	scannedBytes := 41

	var gotTrailer metadata.MD = nil

	stream := mock.NewResponseStream()
	stream.SetTrailerFunc = func(trailer metadata.MD) {
		if gotTrailer != nil {
			t.Error("trailer expected to be set once, but SetTrailer was called more than once")
		} else {
			gotTrailer = trailer
		}
	}

	gc := mock.NewGroupCursor()
	gc.StatsFunc = func() cursors.CursorStats {
		return cursors.CursorStats{
			ScannedValues: scannedValues,
			ScannedBytes:  scannedBytes,
		}
	}
	cNextHasBeenCalledOnce := false
	gc.NextFunc = func() bool {
		if !cNextHasBeenCalledOnce {
			cNextHasBeenCalledOnce = true
			return true
		}
		return false
	}
	gc.CursorFunc = func() cursors.Cursor {
		return mock.NewIntegerArrayCursor()
	}

	rs := mock.NewGroupResultSet()
	rsNextHasBeenCalledOnce := false
	rs.NextFunc = func() reads.GroupCursor {
		if !rsNextHasBeenCalledOnce {
			rsNextHasBeenCalledOnce = true
			return gc
		}
		return nil
	}

	// This is what we're testing.
	rw := reads.NewResponseWriter(stream, 0)
	err := rw.WriteGroupResultSet(rs)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(gotTrailer.Get("scanned-values"), []string{fmt.Sprint(scannedValues)}) {
		t.Errorf("expected scanned-values '%v' but got '%v'", []string{fmt.Sprint(scannedValues)}, gotTrailer.Get("scanned-values"))
	}
	if !reflect.DeepEqual(gotTrailer.Get("scanned-bytes"), []string{fmt.Sprint(scannedBytes)}) {
		t.Errorf("expected scanned-bytes '%v' but got '%v'", []string{fmt.Sprint(scannedBytes)}, gotTrailer.Get("scanned-bytes"))
	}
}
