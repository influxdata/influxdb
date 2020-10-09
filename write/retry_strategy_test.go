package write

import (
	"context"
	"errors"
	"testing"
	"time"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/stretchr/testify/require"
)

func TestDefaultRetryStrategy(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		retryAttempt int
		want         time.Duration // in seconds
	}{
		{
			name: "nil error",
			err:  nil,
			want: 0,
		},
		{
			name: "context cancelled",
			err:  context.Canceled,
			want: 0,
		},
		{
			name: "unrecognized error",
			err:  errors.New("whatever"),
			want: 0,
		},
		{
			name: "Internal Error (0)",
			err: &platform.Error{
				Code: platform.EInternal,
			},
			retryAttempt: 0,
			want:         5,
		},
		{
			name: "Too Many Requests (1)",
			err: &platform.Error{
				Code: platform.ETooManyRequests,
			},
			retryAttempt: 1,
			want:         25,
		},
		{
			name: "Service Not Available (2)",
			err: &platform.Error{
				Code: platform.EUnavailable,
			},
			retryAttempt: 2,
			want:         125,
		},
		{
			name: "Service Not Available (3)",
			err: &platform.Error{
				Code: platform.EUnavailable,
			},
			retryAttempt: 3,
			want:         0,
		},
		{
			name: "Respect Retry-After value",
			err: &platform.Error{
				Code: platform.ETooManyRequests,
				Err:  http.NewRetriableError(nil, 12*time.Second),
			},
			retryAttempt: 1,
			want:         12,
		},
		{
			name: "Respect Zero Retry-After value",
			err: &platform.Error{
				Code: platform.EInternal,
				Err:  http.NewRetriableError(nil, 0),
			},
			retryAttempt: 1,
			want:         0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, DefaultRetryStrategy(tc.err, tc.retryAttempt)/time.Second)
		})
	}
}

func Test_retry(t *testing.T) {
	fakeErr := errors.New("Some Error")
	tests := []struct {
		name      string
		results   []error
		callCount int
		err       error
	}{
		{
			name:      "no retry required",
			results:   []error{nil},
			callCount: 1,
		},
		{
			name:      "one retry error",
			results:   []error{fakeErr, nil},
			callCount: 2,
		},
		{
			name:      "context canceled",
			results:   []error{fakeErr, context.Canceled, nil},
			callCount: 2,
			err:       context.Canceled,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			callCount := 0
			start := time.Now()
			err := retry(ctx, func(err error, attempt int) time.Duration {
				if callCount == len(tc.results) {
					return 0
				}
				if err == context.Canceled {
					return time.Second // we shall not wait a second, because the context is canceled
				}
				return 1
			}, func() error {
				err := tc.results[callCount]
				callCount++
				if err == context.Canceled {
					cancel()
				}
				return err
			})
			require.Less(t, int64(time.Since(start)), int64(time.Second))
			require.Equal(t, tc.callCount, callCount)
			require.Equal(t, tc.err, err)

		})
	}

}
