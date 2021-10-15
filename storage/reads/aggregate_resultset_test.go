package reads_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

func TestNewWindowAggregateResultSet_Tags(t *testing.T) {

	newCursor := sliceSeriesCursor{
		rows: newSeriesRows(
			"clicks click=1 1",
		)}

	request := datatypes.ReadWindowAggregateRequest{
		Aggregate: []*datatypes.Aggregate{
			{
				Type: datatypes.Aggregate_AggregateTypeMean,
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
			2678400000000000,
			5000000000000000,
			5097600000000001,
		},
		Values: []int64{100, 55, 256, 83, 99, 124, 1979, 4, 67, 49929, 51000},
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
	newCursorFn func(req *cursors.CursorRequest) cursors.Cursor
	statsFn     func() cursors.CursorStats
}

func (i *mockCursorIterator) Next(ctx context.Context, req *cursors.CursorRequest) (cursors.Cursor, error) {
	return i.newCursorFn(req), nil
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
		var itrs cursors.CursorIterators
		cur := &mockCursorIterator{
			newCursorFn: func(req *cursors.CursorRequest) cursors.Cursor {
				return &mockIntegerArrayCursor{}
			},
			statsFn: func() cursors.CursorStats {
				return cursors.CursorStats{ScannedBytes: 500, ScannedValues: 10}
			},
		}
		itrs = append(itrs, cur)
		rows[i].Query = itrs
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
				Type: datatypes.Aggregate_AggregateTypeMean,
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

// A mean window aggregate is supported
func TestNewWindowAggregateResultSet_Mean(t *testing.T) {

	newCursor := newMockReadCursor(
		"clicks click=1 1",
	)

	request := datatypes.ReadWindowAggregateRequest{
		Aggregate: []*datatypes.Aggregate{
			&datatypes.Aggregate{Type: datatypes.Aggregate_AggregateTypeMean},
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
	floatArrayCursor := cursor.(cursors.FloatArrayCursor)
	floatArray := floatArrayCursor.Next()

	if !reflect.DeepEqual(floatArray.Timestamps, []int64{1000000010, 1000000020, 1000000030, 2678400000000010, 5000000000000010, 5097600000000010}) {
		t.Log(time.Unix(0, floatArray.Timestamps[0]))
		t.Errorf("unexpected mean timestamps: %v", floatArray.Timestamps)
	}
	if !reflect.DeepEqual(floatArray.Values, []float64{77.5, 508.2, 4, 67, 49929, 51000}) {
		t.Errorf("unexpected mean values: %v", floatArray.Values)
	}
}

func TestNewWindowAggregateResultSet_Months(t *testing.T) {

	newCursor := newMockReadCursor(
		"clicks click=1 1",
	)
	request := datatypes.ReadWindowAggregateRequest{
		Aggregate: []*datatypes.Aggregate{
			&datatypes.Aggregate{Type: datatypes.Aggregate_AggregateTypeMean},
		},
		Window: &datatypes.Window{
			Every: &datatypes.Duration{
				Nsecs:    0,
				Months:   1,
				Negative: false,
			},
		},
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
	floatArrayCursor := cursor.(cursors.FloatArrayCursor)
	floatArray := floatArrayCursor.Next()

	if !reflect.DeepEqual(floatArray.Timestamps, []int64{2678400000000000, 5097600000000000, 7776000000000000}) {
		t.Log(time.Unix(0, floatArray.Timestamps[0]))
		t.Errorf("unexpected month timestamps: %v", floatArray.Timestamps)
	}
	if !reflect.DeepEqual(floatArray.Values, []float64{337.5, 24998, 51000}) {
		t.Errorf("unexpected month values: %v", floatArray.Values)
	}
}

func TestNewWindowAggregateResultSet_UnsupportedTyped(t *testing.T) {
	newCursor := newMockReadCursor(
		"clicks click=1 1",
	)
	for i := range newCursor.rows[0].Query {
		newCursor.rows[0].Query[i] = &mockCursorIterator{
			newCursorFn: func(req *cursors.CursorRequest) cursors.Cursor {
				return &mockStringArrayCursor{}
			},
		}
	}

	request := datatypes.ReadWindowAggregateRequest{
		Aggregate: []*datatypes.Aggregate{
			{Type: datatypes.Aggregate_AggregateTypeMean},
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

func TestNewWindowAggregateResultSet_TimeRange(t *testing.T) {
	newCursor := newMockReadCursor(
		"clicks click=1 1",
	)
	for i := range newCursor.rows[0].Query {
		newCursor.rows[0].Query[i] = &mockCursorIterator{
			newCursorFn: func(req *cursors.CursorRequest) cursors.Cursor {
				if want, got := int64(0), req.StartTime; want != got {
					t.Errorf("unexpected start time -want/+got:\n\t- %d\n\t+ %d", want, got)
				}
				if want, got := int64(29), req.EndTime; want != got {
					t.Errorf("unexpected end time -want/+got:\n\t- %d\n\t+ %d", want, got)
				}
				return &mockIntegerArrayCursor{}
			},
		}
	}

	ctx := context.Background()
	req := datatypes.ReadWindowAggregateRequest{
		Range: &datatypes.TimestampRange{
			Start: 0,
			End:   30,
		},
		Aggregate: []*datatypes.Aggregate{
			{
				Type: datatypes.Aggregate_AggregateTypeCount,
			},
		},
		Window: &datatypes.Window{
			Every: &datatypes.Duration{Nsecs: int64(time.Minute)},
		},
	}

	resultSet, err := reads.NewWindowAggregateResultSet(ctx, &req, &newCursor)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if !resultSet.Next() {
		t.Fatal("expected result")
	}
}
