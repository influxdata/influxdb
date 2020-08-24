package reads_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

func TestNewWindowAggregateResultSet_Tags(t *testing.T) {

	newCursor := sliceSeriesCursor{
		rows: newSeriesRows(
			"clicks click=1 1",
		)}

	request := datatypes.ReadWindowAggregateRequest{
		Aggregate: []*datatypes.Aggregate{
			{
				Type: datatypes.AggregateTypeCount,
			},
		},
	}
	resultSet, err := reads.NewWindowAggregateResultSet(context.Background(), &request, &newCursor)

	if err != nil {
		t.Fatalf("error creating WindowAggregateResultSet: %s", err)
	}

	// If .Next() was never called, seriesRow is nil and tags are empty.
	expectedTags := "[]"
	if resultSet.Tags().String() != expectedTags {
		t.Errorf("expected tags: %s got: %s", expectedTags, resultSet.Tags().String())
	}

	resultSet.Next()
	expectedTags = "[{_m clicks}]"
	if resultSet.Tags().String() != expectedTags {
		t.Errorf("expected tags: %s got: %s", expectedTags, resultSet.Tags().String())
	}
}

type mockIntegerArrayCursor struct {
	callCount int
}

func (i *mockIntegerArrayCursor) Close()                     {}
func (i *mockIntegerArrayCursor) Err() error                 { return nil }
func (i *mockIntegerArrayCursor) Stats() cursors.CursorStats { return cursors.CursorStats{} }
func (i *mockIntegerArrayCursor) Next() *cursors.IntegerArray {
	if i.callCount == 1 {
		return &cursors.IntegerArray{}
	}
	i.callCount++
	return &cursors.IntegerArray{
		Timestamps: []int64{
			1000000000,
			1000000005,
			1000000010,
			1000000011,
			1000000012,
			1000000013,
			1000000014,
			1000000020,
		},
		Values: []int64{100, 55, 256, 83, 99, 124, 1979, 4, 67, 49929},
	}
}

type mockStringArrayCursor struct{}

func (i *mockStringArrayCursor) Close()                     {}
func (i *mockStringArrayCursor) Err() error                 { return nil }
func (i *mockStringArrayCursor) Stats() cursors.CursorStats { return cursors.CursorStats{} }
func (i *mockStringArrayCursor) Next() *cursors.StringArray {
	return &cursors.StringArray{
		Timestamps: []int64{1000000000},
		Values:     []string{"a"},
	}
}

type mockCursorIterator struct {
	newCursorFn func() cursors.Cursor
	statsFn     func() cursors.CursorStats
}

func (i *mockCursorIterator) Next(ctx context.Context, req *cursors.CursorRequest) (cursors.Cursor, error) {
	return i.newCursorFn(), nil
}
func (i *mockCursorIterator) Stats() cursors.CursorStats {
	if i.statsFn == nil {
		return cursors.CursorStats{}
	}
	return i.statsFn()
}

type mockReadCursor struct {
	rows  []reads.SeriesRow
	index int64
}

func newMockReadCursor(keys ...string) mockReadCursor {
	rows := make([]reads.SeriesRow, len(keys))
	for i := range keys {
		rows[i].Name, rows[i].SeriesTags = models.ParseKeyBytes([]byte(keys[i]))
		rows[i].Tags = rows[i].SeriesTags.Clone()
		rows[i].Query = &mockCursorIterator{
			newCursorFn: func() cursors.Cursor {
				return &mockIntegerArrayCursor{}
			},
			statsFn: func() cursors.CursorStats {
				return cursors.CursorStats{ScannedBytes: 500, ScannedValues: 10}
			},
		}
	}

	return mockReadCursor{rows: rows}
}

func (c *mockReadCursor) Next() *reads.SeriesRow {
	if c.index == int64(len(c.rows)) {
		return nil
	}
	row := c.rows[c.index]
	c.index++
	return &row
}
func (c *mockReadCursor) Close()     {}
func (c *mockReadCursor) Err() error { return nil }

// The stats from a WindowAggregateResultSet are retrieved from the cursor.
func TestNewWindowAggregateResultSet_Stats(t *testing.T) {

	newCursor := newMockReadCursor(
		"clicks click=1 1",
	)

	request := datatypes.ReadWindowAggregateRequest{
		Aggregate: []*datatypes.Aggregate{
			{
				Type: datatypes.AggregateTypeCount,
			},
		},
	}
	resultSet, err := reads.NewWindowAggregateResultSet(context.Background(), &request, &newCursor)

	if err != nil {
		t.Fatalf("error creating WindowAggregateResultSet: %s", err)
	}

	// If .Next() was never called, seriesRow is nil and stats are empty.
	stats := resultSet.Stats()
	if stats.ScannedBytes != 0 || stats.ScannedValues != 0 {
		t.Errorf("expected statistics to be empty")
	}

	resultSet.Next()
	stats = resultSet.Stats()
	if stats.ScannedBytes != 500 {
		t.Errorf("Expected scanned bytes: %d got: %d", 500, stats.ScannedBytes)
	}
	if stats.ScannedValues != 10 {
		t.Errorf("Expected scanned values: %d got: %d", 10, stats.ScannedValues)
	}
}

// A count window aggregate is supported
func TestNewWindowAggregateResultSet_Count(t *testing.T) {

	newCursor := newMockReadCursor(
		"clicks click=1 1",
	)

	request := datatypes.ReadWindowAggregateRequest{
		Aggregate: []*datatypes.Aggregate{
			&datatypes.Aggregate{Type: datatypes.AggregateTypeCount},
		},
		WindowEvery: 10,
	}
	resultSet, err := reads.NewWindowAggregateResultSet(context.Background(), &request, &newCursor)

	if err != nil {
		t.Fatalf("error creating WindowAggregateResultSet: %s", err)
	}

	if !resultSet.Next() {
		t.Fatalf("unexpected: resultSet could not advance")
	}
	cursor := resultSet.Cursor()
	if cursor == nil {
		t.Fatalf("unexpected: cursor was nil")
	}
	integerArrayCursor := cursor.(cursors.IntegerArrayCursor)
	integerArray := integerArrayCursor.Next()

	if !reflect.DeepEqual(integerArray.Timestamps, []int64{1000000010, 1000000020, 1000000030}) {
		t.Errorf("unexpected count values: %v", integerArray.Timestamps)
	}
	if !reflect.DeepEqual(integerArray.Values, []int64{2, 5, 1}) {
		t.Errorf("unexpected count values: %v", integerArray.Values)
	}
}

func TestNewWindowAggregateResultSet_UnsupportedTyped(t *testing.T) {
	newCursor := newMockReadCursor(
		"clicks click=1 1",
	)
	newCursor.rows[0].Query = &mockCursorIterator{
		newCursorFn: func() cursors.Cursor {
			return &mockStringArrayCursor{}
		},
	}

	request := datatypes.ReadWindowAggregateRequest{
		Aggregate: []*datatypes.Aggregate{
			{Type: datatypes.AggregateTypeMean},
		},
		WindowEvery: 10,
	}
	resultSet, err := reads.NewWindowAggregateResultSet(context.Background(), &request, &newCursor)

	if err != nil {
		t.Fatalf("error creating WindowAggregateResultSet: %s", err)
	}

	if resultSet.Next() {
		t.Fatal("unexpected: resultSet should not have advanced")
	}
	err = resultSet.Err()
	if err == nil {
		t.Fatal("expected error")
	}
	if want, got := "unsupported input type for mean aggregate: string", err.Error(); want != got {
		t.Fatalf("unexpected error:\n\t- %q\n\t+ %q", want, got)
	}
}
