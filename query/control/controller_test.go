package control

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/plan"
)

// testQuerySpec is a spec that can be used for queries.
var testQuerySpec *query.Spec

func TestController_CancelQuery(t *testing.T) {
	done := make(chan struct{})

	executor := mock.NewExecutor()
	executor.ExecuteFn = func(context.Context, platform.ID, *plan.PlanSpec, *execute.Allocator) (map[string]query.Result, error) {
		<-done
		return nil, nil
	}

	ctrl := New(Config{})
	ctrl.executor = executor

	var id platform.ID
	// Run a query that will cause the controller to stall.
	q, err := ctrl.Query(context.Background(), id, testQuerySpec)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer func() {
		close(done)
		<-q.Ready()
		q.Done()
	}()

	// Run another query. It should block in the Query call and then unblock when we cancel
	// the context.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-timer.C:
		case <-done:
			timer.Stop()
		}
	}()

	if _, err := ctrl.Query(ctx, id, testQuerySpec); err == nil {
		t.Fatal("expected error")
	} else if got, want := err, context.Canceled; got != want {
		t.Fatalf("unexpected error: got=%q want=%q", got, want)
	}
}

func init() {
	spec, err := query.Compile(context.Background(), `from(bucket: "telegraf") |> range(start: -5m) |> mean()`, time.Now())
	if err != nil {
		panic(err)
	}
	testQuerySpec = spec
}
