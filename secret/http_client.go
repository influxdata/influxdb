package secret

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

type Client struct {
	Client *httpc.Client
}

// LoadSecret is not implemented for http
func (s *Client) LoadSecret(ctx context.Context, orgID platform.ID, k string) (string, error) {
	return "", &errors.Error{
		Code: errors.EMethodNotAllowed,
		Msg:  "load secret is not implemented for http",
	}
}

// PutSecret is not implemented for http.
func (s *Client) PutSecret(ctx context.Context, orgID platform.ID, k string, v string) error {
	return &errors.Error{
		Code: errors.EMethodNotAllowed,
		Msg:  "put secret is not implemented for http",
	}
}

// GetSecretKeys get all secret keys mathing an org ID via HTTP.
func (s *Client) GetSecretKeys(ctx context.Context, orgID platform.ID) ([]string, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	span.LogKV("org-id", orgID)

	path := fmt.Sprintf("/api/v2/orgs/%s/secrets", orgID.String())

	var ss secretsResponse
	err := s.Client.
		Get(path).
		DecodeJSON(&ss).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return ss.Secrets, nil
}

// PutSecrets is not implemented for http.
func (s *Client) PutSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error {
	return &errors.Error{
		Code: errors.EMethodNotAllowed,
		Msg:  "put secrets is not implemented for http",
	}
}

// PatchSecrets will update the existing secret with new via http.
func (s *Client) PatchSecrets(ctx context.Context, orgID platform.ID, m map[string]string) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if orgID != 0 {
		span.LogKV("org-id", orgID)
	}

	path := fmt.Sprintf("/api/v2/orgs/%s/secrets", orgID.String())

	return s.Client.
		PatchJSON(m, path).
		Do(ctx)
}

// DeleteSecret removes a single secret via HTTP.
func (s *Client) DeleteSecret(ctx context.Context, orgID platform.ID, ks ...string) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	path := fmt.Sprintf("/api/v2/orgs/%s/secrets/delete", orgID.String())
	return s.Client.
		PostJSON(secretsDeleteBody{
			Secrets: ks,
		}, path).
		Do(ctx)
}
