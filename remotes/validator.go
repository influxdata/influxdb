package remotes

import (
	"context"

	"github.com/influxdata/influxdb/v2"
)

var _ RemoteConnectionValidator = (*stubValidator)(nil)

type stubValidator struct{}

func (n stubValidator) ValidateRemoteConnectionHTTPConfig(context.Context, *influxdb.RemoteConnectionHTTPConfig) error {
	return errNotImplemented
}
