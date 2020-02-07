package precreator_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/influxdata/influxdb/internal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/precreator"
	"github.com/influxdata/influxdb/toml"
)

func TestShardPrecreation(t *testing.T) {
	precreate := false

	var mc internal.MetaClientMock

	ctx, cancel := context.WithCancel(context.Background())

	mc.PrecreateShardGroupsFn = func(now, cutoff time.Time) error {
		if !precreate {
			cancel()
			precreate = true
		}
		return nil
	}

	s := NewTestService()
	s.MetaClient = &mc

	errChan := make(chan error)

	go func() { errChan <- s.Run(ctx, nil) }()

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("s.Run() returned %v; expected nil", err)
		}
	case <-ctx.Done():
		timer.Stop()

	case <-timer.C:
		t.Errorf("timeout exceeded while waiting for precreate")
	}
}

func NewTestService() *precreator.Service {
	config := precreator.NewConfig()
	config.CheckInterval = toml.Duration(10 * time.Millisecond)

	s := precreator.NewService(config)
	s.WithLogger(logger.New(os.Stderr))
	return s
}
