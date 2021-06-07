package limiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2/pkg/limiter"
	"github.com/stretchr/testify/require"
)

func TestFixed_Available(t *testing.T) {
	f := limiter.NewFixed(10)
	require.Equal(t, 10, f.Available())

	require.NoError(t, f.Take(context.Background()))
	require.Equal(t, 9, f.Available())

	f.Release()
	require.Equal(t, 10, f.Available())
}

func TestFixed_Timeout(t *testing.T) {
	f := limiter.NewFixed(1)
	require.NoError(t, f.Take(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := f.Take(ctx)
	require.Error(t, err)
	require.Equal(t, "context deadline exceeded", err.Error())
}

func TestFixed_Canceled(t *testing.T) {
	f := limiter.NewFixed(1)
	require.NoError(t, f.Take(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 30 * time.Second)
	go func() {
		time.Sleep(time.Second)
		cancel()
	}()
	err := f.Take(ctx)
	require.Error(t, err)
	require.Equal(t, "context canceled", err.Error())
}
