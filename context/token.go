package context

import (
	"context"
	"net/http"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/kit/errors"
)

type contextKey string

const (
	authorizationCtxKey = contextKey("influx/authorization/v1")
	tokenCtxKey         = contextKey("influx/token/v1")
)

// SetAuthorization sets an authorization on context.
func SetAuthorization(ctx context.Context, a *platform.Authorization) context.Context {
	return context.WithValue(ctx, authorizationCtxKey, a)
}

// GetAuthorization retrieves an authorization from context.
func GetAuthorization(ctx context.Context) (*platform.Authorization, error) {
	a, ok := ctx.Value(authorizationCtxKey).(*platform.Authorization)
	if !ok {
		return nil, errors.InternalErrorf("authorization not found on context")
	}

	return a, nil
}

// SetToken sets an authorization on context.
func SetToken(ctx context.Context, t string) context.Context {
	return context.WithValue(ctx, tokenCtxKey, t)
}

// GeToken retrieves an authorization from context.
func GetToken(ctx context.Context) (string, error) {
	t, ok := ctx.Value(tokenCtxKey).(string)
	if !ok {
		return "", errors.InternalErrorf("token not found on context")
	}

	return t, nil
}

// TokenFromAuthorizationHeader retrieves a auth token from the HTTP Authorization header.
var TokenFromAuthorizationHeader = func(ctx context.Context, r *http.Request) context.Context {
	token := r.Header.Get("Authorization")
	return SetToken(ctx, token)
}
