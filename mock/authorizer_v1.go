package mock

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

type AuthorizerV1 struct {
	AuthorizeFn func(ctx context.Context, c influxdb.CredentialsV1) (*influxdb.Authorization, error)
}

func (a *AuthorizerV1) Authorize(ctx context.Context, c influxdb.CredentialsV1) (*influxdb.Authorization, error) {
	return a.AuthorizeFn(ctx, c)
}
