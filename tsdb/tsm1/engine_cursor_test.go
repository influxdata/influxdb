package tsm1_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

func TestEngine_CursorIterator_Stats(t *testing.T) {
	e := MustOpenEngine(t)
	defer e.Close()

	points := []models.Point{
		models.MustNewPoint("cpu",
			models.Tags{
				{Key: []byte("a"), Value: []byte("b")},
			},
			models.Fields{"value": 4.6},
			time.Now().UTC(),
		),
		models.MustNewPoint("cpu",
			models.Tags{
				{Key: []byte("a"), Value: []byte("b")},
			},
			models.Fields{"value": 3.2},
			time.Now().UTC(),
		),
		models.MustNewPoint("mem",
			models.Tags{
				{Key: []byte("b"), Value: []byte("c")},
			},
			models.Fields{"value": int64(3)},
			time.Now().UTC(),
		),
	}

	// Write into the index.
	collection := tsdb.NewSeriesCollection(points)
	if err := e.index.CreateSeriesListIfNotExists(collection); err != nil {
		t.Fatal(err)
	}

	if err := e.WritePoints(points); err != nil {
		t.Fatal(err)
	}

	e.MustWriteSnapshot()

	ctx := context.Background()
	cursorIterator, err := e.CreateCursorIterator(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cur := cursorIterator.Next(ctx, &tsdb.CursorRequest{
		Name:      []byte("cpu"),
		Tags:      []models.Tag{{Key: []byte("a"), Value: []byte("b")}},
		Field:     "value",
		EndTime:   time.Now().UTC().UnixNano(),
		Ascending: true,
	})

	if cur == nil {
		t.Fatal("expected cursor to be present")
	}

	fc, ok := cur.(cursors.FloatArrayCursor)
	if !ok {
		t.Fatalf("unexpected cursor type: expected FloatArrayCursor, got %#v", cur)
	}

	// drain the cursor
	for a := fc.Next(); a.Len() > 0; a = fc.Next() {
	}

	cur.Close()

	cur = cursorIterator.Next(ctx, &tsdb.CursorRequest{
		Name:      []byte("mem"),
		Tags:      []models.Tag{{Key: []byte("b"), Value: []byte("c")}},
		Field:     "value",
		EndTime:   time.Now().UTC().UnixNano(),
		Ascending: true,
	})

	if cur == nil {
		t.Fatal("expected cursor to be present")
	}

	defer cur.Close()

	ic, ok := cur.(cursors.IntegerArrayCursor)
	if !ok {
		t.Fatalf("unexpected cursor type: expected FloatArrayCursor, got %#v", cur)
	}

	// drain the cursor
	for a := ic.Next(); a.Len() > 0; a = ic.Next() {
	}

	// iterator should report integer array stats
	if got, exp := cursorIterator.Stats(), (cursors.CursorStats{ScannedValues: 3, ScannedBytes: 24}); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}
