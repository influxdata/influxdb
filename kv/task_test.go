package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/kv"
	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/influxdb/task/servicetest"
)

func TestInmemTaskService(t *testing.T) {
	servicetest.TestTaskService(
		t,
		func(t *testing.T) (*servicetest.System, context.CancelFunc) {
			store, close, err := NewTestInmemStore()
			if err != nil {
				t.Fatal(err)
			}

			service := kv.NewService(store)
			ctx, cancelFunc := context.WithCancel(context.Background())

			if err := service.Initialize(ctx); err != nil {
				t.Fatalf("error initializing urm service: %v", err)
			}

			go func() {
				<-ctx.Done()
				close()
			}()

			return &servicetest.System{
				TaskControlService: service,
				TaskService:        service,
				I:                  service,
				Ctx:                ctx,
			}, cancelFunc
		},
		"transactional",
	)
}

func TestBoltTaskService(t *testing.T) {
	servicetest.TestTaskService(
		t,
		func(t *testing.T) (*servicetest.System, context.CancelFunc) {
			store, close, err := NewTestBoltStore()
			if err != nil {
				t.Fatal(err)
			}

			service := kv.NewService(store)
			ctx, cancelFunc := context.WithCancel(context.Background())
			if err := service.Initialize(ctx); err != nil {
				t.Fatalf("error initializing urm service: %v", err)
			}

			go func() {
				<-ctx.Done()
				close()
			}()

			return &servicetest.System{
				TaskControlService: service,
				TaskService:        service,
				I:                  service,
				Ctx:                ctx,
			}, cancelFunc
		},
		"transactional",
	)
}
