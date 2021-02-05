package meta

import (
	"context"
)

type key int

const (
	userKey key = iota
)

// NewContextWithUser returns a new context with user added.
func NewContextWithUser(ctx context.Context, user User) context.Context {
	return context.WithValue(ctx, userKey, user)
}

// UserFromContext returns the User associated with ctx or nil if no user has been assigned.
func UserFromContext(ctx context.Context) User {
	l, _ := ctx.Value(userKey).(User)
	return l
}
