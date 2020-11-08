package write

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http"
)

// RetryStrategy returns time to wait before retrying an operation. The return value is based
// on the error supplied and the count of operation retry attempts that already failed. A zero return
// value indicates that the operation shall not be retried. Note that the retry strategy is also
// informed about a successful operation when called with nil error.
type RetryStrategy func(err error, retryAttempt int) time.Duration

var defaultDelays []time.Duration = []time.Duration{5 * time.Second, 25 * time.Second, 125 * time.Second}

// DefaultRetryStrategy is an implementation of RetryStrategy that respects the server-imposed delay
// sent in RetryAfter HTTP header. If no server-sent delay is available and the error indicates that another
// attempt could be successful (500 or 503 HTTP errors), the delay depends on the retry attempt. 5,25 or
// 125 seconds are used. It allows at most 3 retry attempts. A small jitter is added to
// every returned possitive value.
func DefaultRetryStrategy(err error, retryAttempt int) time.Duration {
	if err == nil || retryAttempt >= len(defaultDelays) || err == context.Canceled {
		// don't retry
		return 0
	}
	retryAfter := http.RetryAfter(err)
	if retryAfter == 0 {
		// don't retry if the server requests so
		return 0
	}
	if retryAfter > 0 {
		return retryAfter + time.Duration(rand.Int63n(200_000_000))
	}
	// retry < 0, an explicit delay is not available
	if pErr, ok := err.(*platform.Error); ok {
		if pErr.Code == platform.EUnavailable || pErr.Code == platform.EInternal || pErr.Code == platform.ETooManyRequests {
			return defaultDelays[retryAttempt] + time.Duration(rand.Int63n(200_000_000))
		}
	}
	return 0
}

// Retry calls the supplied fn until it succeeds
// and can be retried (retryStrategy returns positive value).
func retry(ctx context.Context, retryStrategy RetryStrategy, fn func() error) error {
	retryAttempt := 0
	var err error
	for {
		err = fn()
		// respect the Retry-After value if found in the error
		retryAfter := retryStrategy(err, retryAttempt)
		if retryAfter == 0 {
			// it cannot be retried anymore
			return err
		}
		retryAttempt++
		fmt.Printf("WARN: Write to InfluxDB failed (attempts: %d), retrying after %v : %v\n",
			retryAttempt, retryAfter, err)
		// wait until it can be retried
		select {
		case <-time.After(retryAfter):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
