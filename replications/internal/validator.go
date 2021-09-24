package internal

import (
	"context"
	"fmt"
	"net/url"

	"github.com/influxdata/influx-cli/v2/api"
	ierrors "github.com/influxdata/influxdb/v2/kit/platform/errors"
)

func NewValidator() *noopWriteValidator {
	return &noopWriteValidator{}
}

// noopWriteValidator checks if replication parameters are valid by attempting to write an empty payload
// to the remote host using the configured information.
type noopWriteValidator struct{}

func (s noopWriteValidator) ValidateReplication(ctx context.Context, config *ReplicationHTTPConfig) error {
	u, err := url.Parse(config.RemoteURL)
	if err != nil {
		return &ierrors.Error{
			Code: ierrors.EInvalid,
			Msg:  fmt.Sprintf("host URL %q is invalid", config.RemoteURL),
			Err:  err,
		}
	}
	params := api.ConfigParams{
		Host:             u,
		UserAgent:        "",
		Token:            &config.RemoteToken,
		AllowInsecureTLS: config.AllowInsecureTLS,
	}
	client := api.NewAPIClient(api.NewAPIConfig(params)).WriteApi

	noopReq := client.PostWrite(ctx).
		Org(config.RemoteOrgID.String()).
		Bucket(config.RemoteBucketID.String()).
		Body([]byte{})

	if err := noopReq.Execute(); err != nil {
		return &ierrors.Error{
			Code: ierrors.EInvalid,
			Err:  err,
		}
	}
	return nil
}
