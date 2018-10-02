package context

import (
	"context"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/kit/errors"
)

type contextKey string

const (
	authorizationCtxKey = contextKey("influx/authorization/v1")
	authorizerCtxKey    = contextKey("influx/authorizer/v1")
	tokenCtxKey         = contextKey("influx/token/v1")
)

// SetAuthorizer sets an authorizer on context.
func SetAuthorizer(ctx context.Context, a platform.Authorizer) context.Context {
	return context.WithValue(ctx, authorizerCtxKey, a)
}

// GetAuthorizer retrieves an authorizer from context.
func GetAuthorizer(ctx context.Context) (platform.Authorizer, error) {
	a, ok := ctx.Value(authorizerCtxKey).(platform.Authorizer)
	if !ok {
		return nil, errors.InternalErrorf("authorizer not found on context")
	}

	return a, nil
}

// SetToken sets an token on context.
func SetToken(ctx context.Context, t string) context.Context {
	return context.WithValue(ctx, tokenCtxKey, t)
}

// GeToken retrieves an token from context.
func GetToken(ctx context.Context) (string, error) {
	t, ok := ctx.Value(tokenCtxKey).(string)
	if !ok {
		return "", errors.InternalErrorf("token not found on context")
	}

	return t, nil
}

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
