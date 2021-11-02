package reads_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/storage/reads/datatypes"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

func TestNewFilteredResultSet_TimeRange(t *testing.T) {
	newCursor := newMockReadCursor(
		"clicks click=1 1",
	)
	for i := range newCursor.rows {
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
	req := datatypes.ReadFilterRequest{
		Range: &datatypes.TimestampRange{
			Start: 0,
			End:   30,
		},
	}

	resultSet := reads.NewFilteredResultSet(ctx, req.Range.GetStart(), req.Range.GetEnd(), &newCursor)
	if !resultSet.Next() {
		t.Fatal("expected result")
	}
}
