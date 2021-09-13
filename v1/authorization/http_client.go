package authorization

import (
	"context"
	"errors"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

var (
	_ influxdb.AuthorizationService = (*Client)(nil)
	_ PasswordService               = (*Client)(nil)
)

// Client connects to Influx via HTTP using tokens to manage authorizations
type Client struct {
	Client *httpc.Client
}

// CreateAuthorization creates a new authorization and sets b.ID with the new identifier.
func (s *Client) CreateAuthorization(ctx context.Context, a *influxdb.Authorization) error {
	newAuth, err := newPostAuthorizationRequest(a)
	if err != nil {
		return err
	}

	return s.Client.
		PostJSON(newAuth, prefixAuthorization).
		DecodeJSON(a).
		Do(ctx)
}

// FindAuthorizations returns a list of authorizations that match filter and the total count of matching authorizations.
// Additional options provide pagination & sorting.
func (s *Client) FindAuthorizations(ctx context.Context, filter influxdb.AuthorizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
	params := influxdb.FindOptionParams(opt...)
	if filter.ID != nil {
		params = append(params, [2]string{"id", filter.ID.String()})
	}
	if filter.Token != nil {
		params = append(params, [2]string{"token", *filter.Token})
	}
	if filter.UserID != nil {
		params = append(params, [2]string{"userID", filter.UserID.String()})
	}
	if filter.User != nil {
		params = append(params, [2]string{"user", *filter.User})
	}
	if filter.OrgID != nil {
		params = append(params, [2]string{"orgID", filter.OrgID.String()})
	}
	if filter.Org != nil {
		params = append(params, [2]string{"org", *filter.Org})
	}

	var as authsResponse
	err := s.Client.
		Get(prefixAuthorization).
		QueryParams(params...).
		DecodeJSON(&as).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	auths := make([]*influxdb.Authorization, 0, len(as.Auths))
	for _, a := range as.Auths {
		auths = append(auths, a.toInfluxdb())
	}

	return auths, len(auths), nil
}

// FindAuthorizationByToken is not supported by the HTTP authorization service.
func (s *Client) FindAuthorizationByToken(ctx context.Context, token string) (*influxdb.Authorization, error) {
	return nil, errors.New("not supported in HTTP authorization service")
}

// FindAuthorizationByID finds a single Authorization by its ID against a remote influx server.
func (s *Client) FindAuthorizationByID(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
	var b influxdb.Authorization
	err := s.Client.
		Get(prefixAuthorization, id.String()).
		DecodeJSON(&b).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// UpdateAuthorization updates the status and description if available.
func (s *Client) UpdateAuthorization(ctx context.Context, id platform.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
	var res authResponse
	err := s.Client.
		PatchJSON(upd, prefixAuthorization, id.String()).
		DecodeJSON(&res).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return res.toInfluxdb(), nil
}

// DeleteAuthorization removes a authorization by id.
func (s *Client) DeleteAuthorization(ctx context.Context, id platform.ID) error {
	return s.Client.
		Delete(prefixAuthorization, id.String()).
		Do(ctx)
}

// SetPassword sets the password for the authorization token id.
func (s *Client) SetPassword(ctx context.Context, id platform.ID, password string) error {
	return s.Client.
		PostJSON(passwordSetRequest{
			Password: password,
		}, prefixAuthorization, id.String(), "password").
		Do(ctx)
}
