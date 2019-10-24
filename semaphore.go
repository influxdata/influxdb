package influxdb

import (
	"context"
	"errors"
	"time"
)

// ErrNoAcquire is returned when it was not possible to acquire ownership of the
// semaphore.
var ErrNoAcquire = errors.New("ownership not acquired")

// DefaultLeaseTTL is used when a specific lease TTL is not requested.
const DefaultLeaseTTL = time.Minute

// A Semaphore provides an API for requesting ownership of an expirable semaphore.
//
// Acquired semaphores have an expiration. If they're not released or kept alive
// during this period then they will expire and ownership of the semaphore will
// be lost.
//
// TODO(edd): add AcquireTTL when needed. It should block.
type Semaphore interface {
	// TryAcquire attempts to acquire ownership of the semaphore. TryAcquire
	// must not block. Failure to get ownership of the semaphore should be
	// signalled to the caller via the return of the ErrNoAcquire error.
	TryAcquire(ctx context.Context, ttl time.Duration) (Lease, error)
}

// A Lease represents ownership over a semaphore. It gives the owner the ability
// to extend ownership over the semaphore or release ownership of the semaphore.
type Lease interface {
	// TTL returns the duration of time remaining before the lease expires.
	TTL(context.Context) (time.Duration, error)

	// Release terminates ownership of the semaphore by revoking the lease.
	Release(context.Context) error

	// KeepAlive extends the lease back to the original TTL.
	KeepAlive(context.Context) error
}

// NopSemaphore is a Semaphore that always hands out leases.
var NopSemaphore Semaphore = nopSemaphore{}

type nopSemaphore struct{}

func (nopSemaphore) TryAcquire(ctx context.Context, ttl time.Duration) (Lease, error) {
	return nopLease{}, nil
}

type nopLease struct{}

func (nopLease) TTL(context.Context) (time.Duration, error) { return DefaultLeaseTTL, nil }
func (nopLease) Release(context.Context) error              { return nil }
func (nopLease) KeepAlive(context.Context) error            { return nil }
