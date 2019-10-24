package metrics

import "context"

type groupContextKey struct{}

// NewContextWithGroup returns a new context with the given Group added.
func NewContextWithGroup(ctx context.Context, c *Group) context.Context {
	return context.WithValue(ctx, groupContextKey{}, c)
}

// GroupFromContext returns the Group associated with ctx or nil if no Group has been assigned.
func GroupFromContext(ctx context.Context) *Group {
	c, _ := ctx.Value(groupContextKey{}).(*Group)
	return c
}
