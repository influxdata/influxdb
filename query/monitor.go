package query

import (
	"context"
	"time"
)

// MonitorFunc is a function that will be called to check if a query
// is currently healthy. If the query needs to be interrupted for some reason,
// the error should be returned by this function.
type MonitorFunc func(<-chan struct{}) error

// Monitor monitors the status of a query and returns whether the query should
// be aborted with an error.
type Monitor interface {
	// Monitor starts a new goroutine that will monitor a query. The function
	// will be passed in a channel to signal when the query has been finished
	// normally. If the function returns with an error and the query is still
	// running, the query will be terminated.
	Monitor(fn MonitorFunc)
}

// MonitorFromContext returns a Monitor embedded within the Context
// if one exists.
func MonitorFromContext(ctx context.Context) Monitor {
	v, _ := ctx.Value(monitorContextKey).(Monitor)
	return v
}

// PointLimitMonitor is a query monitor that exits when the number of points
// emitted exceeds a threshold.
func PointLimitMonitor(cur Cursor, interval time.Duration, limit int) MonitorFunc {
	return func(closing <-chan struct{}) error {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				stats := cur.Stats()
				if stats.PointN >= limit {
					return ErrMaxSelectPointsLimitExceeded(stats.PointN, limit)
				}
			case <-closing:
				return nil
			}
		}
	}
}
