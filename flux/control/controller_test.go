package control_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/memory"
	"github.com/influxdata/flux/mock"
	"github.com/influxdata/influxdb/flux/control"
	"github.com/influxdata/influxdb/internal"
	imock "github.com/influxdata/influxdb/mock"
	"go.uber.org/zap/zaptest"
)

func TestController_Query(t *testing.T) {
	mc := &internal.MetaClientMock{}
	reader := &imock.Reader{}
	ctrl := control.NewController(mc, reader, nil, false, zaptest.NewLogger(t))

	t.Run("stats", func(t *testing.T) {
		ctx := context.Background()
		compiler := &mock.Compiler{
			Type: "mock",
			CompileFn: func(ctx context.Context) (flux.Program, error) {
				time.Sleep(time.Second)
				return &mock.Program{
					StartFn: func(ctx context.Context, alloc *memory.Allocator) (*mock.Query, error) {
						ch := make(chan flux.Result)
						close(ch)
						q := &mock.Query{
							ResultsCh: ch,
						}
						q.SetStatistics(flux.Statistics{
							MaxAllocated:   1025,
							TotalAllocated: 2049,
						})
						return q, nil
					},
				}, nil
			},
		}
		q, err := ctrl.Query(ctx, compiler)
		if err != nil {
			t.Fatal(err)
		}

		for range q.Results() {
		}
		q.Done()

		gotStats := q.Statistics()
		if w, g := int64(1025), gotStats.MaxAllocated; w != g {
			t.Errorf("wanted %d max bytes allocated, got %d", w, g)
		}
		if w, g := int64(2049), gotStats.TotalAllocated; w != g {
			t.Errorf("wanted %d total bytes allocated, got %d", w, g)
		}

		if g := gotStats.CompileDuration; g <= 0 {
			t.Errorf("wanted compile duration to be greater than zero, got %d", g)
		}
		if g := gotStats.ExecuteDuration; g <= 0 {
			t.Errorf("wanted execute duration to be greater than zero, got %d", g)
		}
		if g, w := gotStats.TotalDuration, gotStats.CompileDuration+gotStats.ExecuteDuration; g <= w {
			t.Errorf("wanted total duration to be greater than or equal to %d, got %d", w, g)
		}
	})
}
