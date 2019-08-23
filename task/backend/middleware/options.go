package middleware

import "time"

// Option is a functional option for the coordinating task service
type Option func(*CoordinatingTaskService)

// WithNowFunc sets the now func used to derive time
func WithNowFunc(fn func() time.Time) Option {
	return func(c *CoordinatingTaskService) {
		c.now = fn
	}
}
