// Package limiter provides concurrency limiters.
package limiter

// Fixed is a simple channel-based concurrency limiter.  It uses a fixed
// size channel to limit callers from proceeding until there is a value available
// in the channel.  If all are in-use, the caller blocks until one is freed.
type Fixed chan struct{}

func NewFixed(limit int) Fixed {
	return make(Fixed, limit)
}

// Idle returns true if the limiter has all its capacity is available.
func (t Fixed) Idle() bool {
	return len(t) == cap(t)
}

// Available returns the number of available tokens that may be taken.
func (t Fixed) Available() int {
	return cap(t) - len(t)
}

// Capacity returns the number of tokens can be taken.
func (t Fixed) Capacity() int {
	return cap(t)
}

// TryTake attempts to take a token and return true if successful, otherwise returns false.
func (t Fixed) TryTake() bool {
	select {
	case t <- struct{}{}:
		return true
	default:
		return false
	}
}

// Take attempts to take a token and blocks until one is available.
func (t Fixed) Take() {
	t <- struct{}{}
}

// Release releases a token back to the limiter.
func (t Fixed) Release() {
	<-t
}
