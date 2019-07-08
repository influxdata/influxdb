package context

import (
	"context"
	"fmt"

	platform "github.com/influxdata/influxdb"
)

type contextKey string

const (
	authorizerCtxKey = contextKey("influx/authorizer/v1")
)

// SetAuthorizer sets an authorizer on context.
func SetAuthorizer(ctx context.Context, a platform.Authorizer) context.Context {
	return context.WithValue(ctx, authorizerCtxKey, a)
}

// GetAuthorizer retrieves an authorizer from context.
func GetAuthorizer(ctx context.Context) (platform.Authorizer, error) {
	a, ok := ctx.Value(authorizerCtxKey).(platform.Authorizer)
	if !ok {
		return nil, &platform.Error{
			Msg:  "authorizer not found on context",
			Code: platform.EInternal,
		}
	}

	return a, nil
}

// GetToken retrieves a token from the context; errors if no token.
func GetToken(ctx context.Context) (string, error) {
	a, ok := ctx.Value(authorizerCtxKey).(platform.Authorizer)
	if !ok {
		return "", &platform.Error{
			Msg:  "authorizer not found on context",
			Code: platform.EInternal,
		}
	}

	auth, ok := a.(*platform.Authorization)
	if !ok {
		return "", &platform.Error{
			Msg:  fmt.Sprintf("authorizer not an authorization but a %T", a),
			Code: platform.EInternal,
		}
	}

	return auth.Token, nil
}
