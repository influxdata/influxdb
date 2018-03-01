package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
)

func TestPointLimitMonitor(t *testing.T) {
	t.Parallel()

	stmt := MustParseSelectStatement(`SELECT mean(value) FROM cpu`)

	// Create a new task manager so we can use the query task as a monitor.
	taskManager := query.NewTaskManager()
	ctx, detach, err := taskManager.AttachQuery(&influxql.Query{
		Statements: []influxql.Statement{stmt},
	}, query.ExecutionOptions{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer detach()

	shardMapper := ShardMapper{
		MapShardsFn: func(sources influxql.Sources, t influxql.TimeRange) query.ShardGroup {
			return &ShardGroup{
				CreateIteratorFn: func(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions) (query.Iterator, error) {
					return &FloatIterator{
						Points: []query.FloatPoint{
							{Name: "cpu", Value: 35},
						},
						Context: ctx,
						Delay:   2 * time.Second,
						stats: query.IteratorStats{
							PointN: 10,
						},
					}, nil
				},
				Fields: map[string]influxql.DataType{
					"value": influxql.Float,
				},
			}
		},
	}

	cur, err := query.Select(ctx, stmt, &shardMapper, query.SelectOptions{
		MaxPointN: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if err := query.DrainCursor(cur); err == nil {
		t.Fatalf("expected an error")
	} else if got, want := err.Error(), "max-select-point limit exceeed: (10/1)"; got != want {
		t.Fatalf("unexpected error: got=%v want=%v", got, want)
	}
}
