package context

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2"
)

type contextKey string

const (
	authorizerCtxKey contextKey = "influx/authorizer/v1"
)

// SetAuthorizer sets an authorizer on context.
func SetAuthorizer(ctx context.Context, a influxdb.Authorizer) context.Context {
	return context.WithValue(ctx, authorizerCtxKey, a)
}

// GetAuthorizer retrieves an authorizer from context.
func GetAuthorizer(ctx context.Context) (influxdb.Authorizer, error) {
	a, ok := ctx.Value(authorizerCtxKey).(influxdb.Authorizer)
	if !ok {
		return nil, &influxdb.Error{
			Msg:  "authorizer not found on context",
			Code: influxdb.EInternal,
		}
	}
	if a == nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInternal,
			Msg:  "unexpected invalid authorizer",
		}
	}

	return a, nil
}

// GetToken retrieves a token from the context; errors if no token.
func GetToken(ctx context.Context) (string, error) {
	a, ok := ctx.Value(authorizerCtxKey).(influxdb.Authorizer)
	if !ok {
		return "", &influxdb.Error{
			Msg:  "authorizer not found on context",
			Code: influxdb.EInternal,
		}
	}

	auth, ok := a.(*influxdb.Authorization)
	if !ok {
		return "", &influxdb.Error{
			Msg:  fmt.Sprintf("authorizer not an authorization but a %T", a),
			Code: influxdb.EInternal,
		}
	}

	return auth.Token, nil
}

// GetUserID retrieves the user ID from the authorizer on the context.
func GetUserID(ctx context.Context) (influxdb.ID, error) {
	a, err := GetAuthorizer(ctx)
	if err != nil {
		return 0, err
	}
	return a.GetUserID(), nil
}
