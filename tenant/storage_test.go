package tenant

import "time"

// WithNow is a test only option used to override the now time
// generating function
func WithNow(fn func() time.Time) StoreOption {
	return func(s *Store) {
		s.now = fn
	}
}
