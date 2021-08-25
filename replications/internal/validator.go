package internal

import (
	"context"

	ierrors "github.com/influxdata/influxdb/v2/kit/platform/errors"
)

func NewValidator() *stubValidator {
	return &stubValidator{}
}

type stubValidator struct{}

func (s stubValidator) ValidateReplication(ctx context.Context, config *ReplicationHTTPConfig) error {
	return &ierrors.Error{Code: ierrors.ENotImplemented, Msg: "replication validation not implemented"}
}
