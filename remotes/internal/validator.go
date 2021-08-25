package internal

import (
	"context"

	ierrors "github.com/influxdata/influxdb/v2/kit/platform/errors"
)

func NewValidator() *stubValidator {
	return &stubValidator{}
}

type stubValidator struct{}

func (n stubValidator) ValidateRemoteConnectionHTTPConfig(context.Context, *RemoteConnectionHTTPConfig) error {
	return &ierrors.Error{Code: ierrors.ENotImplemented, Msg: "remote validation not implemented"}
}
