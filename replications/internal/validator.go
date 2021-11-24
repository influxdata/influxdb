package internal

import (
	"context"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/replications/remotewrite"
)

func NewValidator() *noopWriteValidator {
	return &noopWriteValidator{}
}

// noopWriteValidator checks if replication parameters are valid by attempting to write an empty payload
// to the remote host using the configured information.
type noopWriteValidator struct{}

func (s noopWriteValidator) ValidateReplication(ctx context.Context, config *influxdb.ReplicationHTTPConfig) error {
	_, err := remotewrite.PostWrite(ctx, config, []byte{}, remotewrite.DefaultTimeout)
	return err
}
