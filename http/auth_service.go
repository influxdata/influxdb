package http

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

const prefixAuthorization = "/api/v2/authorizations"

type authResponse struct {
	ID          influxdb.ID                `json:"id"`
	Token       string                     `json:"token"`
	Status      influxdb.Status            `json:"status"`
	Type        influxdb.AuthorizationType `json:"authorizationType"`
	Description string                     `json:"description"`
	OrgID       influxdb.ID                `json:"orgID"`
	Org         string                     `json:"org"`
	UserID      influxdb.ID                `json:"userID"`
	User        string                     `json:"user"`
	Permissions []permissionResponse       `json:"permissions"`
	Links       map[string]string          `json:"links"`
	CreatedAt   time.Time                  `json:"createdAt"`
	UpdatedAt   time.Time                  `json:"updatedAt"`
}

func newAuthResponse(a *influxdb.Authorization, org *influxdb.Organization, user *influxdb.User, ps []permissionResponse) *authResponse {
	res := &authResponse{
		ID:          a.ID,
		Token:       a.Token,
		Status:      a.Status,
		Type:        a.Type,
		Description: a.Description,
		OrgID:       a.OrgID,
		UserID:      a.UserID,
		User:        user.Name,
		Org:         org.Name,
		Permissions: ps,
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/authorizations/%s", a.ID),
			"user": fmt.Sprintf("/api/v2/users/%s", a.UserID),
		},
		CreatedAt: a.CreatedAt,
		UpdatedAt: a.UpdatedAt,
	}
	return res
}

func (a *authResponse) toPlatform() *influxdb.Authorization {
	res := &influxdb.Authorization{
		ID:          a.ID,
		Token:       a.Token,
		Status:      a.Status,
		Type:        a.Type,
		Description: a.Description,
		OrgID:       a.OrgID,
		UserID:      a.UserID,
		CRUDLog: influxdb.CRUDLog{
			CreatedAt: a.CreatedAt,
			UpdatedAt: a.UpdatedAt,
		},
	}
	for _, p := range a.Permissions {
		res.Permissions = append(res.Permissions, influxdb.Permission{Action: p.Action, Resource: p.Resource.Resource})
	}
	return res
}

type permissionResponse struct {
	Action   influxdb.Action  `json:"action"`
	Resource resourceResponse `json:"resource"`
}

type resourceResponse struct {
	influxdb.Resource
	Name         string `json:"name,omitempty"`
	Organization string `json:"org,omitempty"`
}

type authsResponse struct {
	Links map[string]string `json:"links"`
	Auths []*authResponse   `json:"authorizations"`
}

type postAuthorizationRequest struct {
	Status      influxdb.Status            `json:"status"`
	Type        influxdb.AuthorizationType `json:"authorizationType"`
	OrgID       influxdb.ID                `json:"orgID"`
	UserID      *influxdb.ID               `json:"userID,omitempty"`
	Description string                     `json:"description"`
	Permissions []influxdb.Permission      `json:"permissions"`
}

func newPostAuthorizationRequest(a *influxdb.Authorization) (*postAuthorizationRequest, error) {
	res := &postAuthorizationRequest{
		OrgID:       a.OrgID,
		Description: a.Description,
		Permissions: a.Permissions,
		Status:      a.Status,
		Type:        a.Type,
	}

	if a.UserID.Valid() {
		res.UserID = &a.UserID
	}

	res.SetDefaults()

	return res, res.Validate()
}

func (p *postAuthorizationRequest) SetDefaults() {
	if p.Status == "" {
		p.Status = influxdb.Active
	}

	if p.Type == "" {
		p.Type = influxdb.AuthorizationTypePlain
	}
}

func (p *postAuthorizationRequest) Validate() error {
	if len(p.Permissions) == 0 {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "authorization must include permissions",
		}
	}

	for _, perm := range p.Permissions {
		if err := perm.Valid(); err != nil {
			return &influxdb.Error{
				Err: err,
			}
		}
	}

	if !p.OrgID.Valid() {
		return &influxdb.Error{
			Err:  influxdb.ErrInvalidID,
			Code: influxdb.EInvalid,
			Msg:  "org id required",
		}
	}

	if p.Status == "" {
		p.Status = influxdb.Active
	}

	if err := p.Status.Valid(); err != nil {
		return err
	}

	if p.Type == "" {
		p.Type = influxdb.AuthorizationTypePlain
	}

	if err := p.Type.Valid(); err != nil {
		return err
	}

	return nil
}

// AuthorizationService connects to Influx via HTTP using tokens to manage authorizations
type AuthorizationService struct {
	Client *httpc.Client
}

var _ influxdb.AuthorizationService = (*AuthorizationService)(nil)

// FindAuthorizationByID finds the authorization against a remote influx server.
func (s *AuthorizationService) FindAuthorizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Authorization, error) {
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

// FindAuthorizationByToken returns a single authorization by Token.
func (s *AuthorizationService) FindAuthorizationByToken(ctx context.Context, t string) (*influxdb.Authorization, error) {
	return nil, errors.New("not supported in HTTP authorization service")
}

// FindAuthorizations returns a list of authorizations that match filter and the total count of matching authorizations.
// Additional options provide pagination & sorting.
func (s *AuthorizationService) FindAuthorizations(ctx context.Context, filter influxdb.AuthorizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Authorization, int, error) {
	params := influxdb.FindOptionParams(opt...)
	if filter.ID != nil {
		params = append(params, [2]string{"id", filter.ID.String()})
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
	if filter.Type != nil {
		params = append(params, [2]string{"authorizationType", string(*filter.Type)})
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
		auths = append(auths, a.toPlatform())
	}

	return auths, len(auths), nil
}

// CreateAuthorization creates a new authorization and sets b.ID with the new identifier.
func (s *AuthorizationService) CreateAuthorization(ctx context.Context, a *influxdb.Authorization) error {
	newAuth, err := newPostAuthorizationRequest(a)
	if err != nil {
		return err
	}

	return s.Client.
		PostJSON(newAuth, prefixAuthorization).
		DecodeJSON(a).
		Do(ctx)
}

// UpdateAuthorization updates the status and description if available.
func (s *AuthorizationService) UpdateAuthorization(ctx context.Context, id influxdb.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
	var res authResponse
	err := s.Client.
		PatchJSON(upd, prefixAuthorization, id.String()).
		DecodeJSON(&res).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return res.toPlatform(), nil
}

// DeleteAuthorization removes a authorization by id.
func (s *AuthorizationService) DeleteAuthorization(ctx context.Context, id influxdb.ID) error {
	return s.Client.
		Delete(prefixAuthorization, id.String()).
		Do(ctx)
}
