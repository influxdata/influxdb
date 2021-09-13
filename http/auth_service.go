package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	platcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"go.uber.org/zap"
)

const prefixAuthorization = "/api/v2/authorizations"

// AuthorizationBackend is all services and associated parameters required to construct
// the AuthorizationHandler.
type AuthorizationBackend struct {
	errors2.HTTPErrorHandler
	log *zap.Logger

	AuthorizationService influxdb.AuthorizationService
	OrganizationService  influxdb.OrganizationService
	UserService          influxdb.UserService
	LookupService        influxdb.LookupService
}

// NewAuthorizationBackend returns a new instance of AuthorizationBackend.
func NewAuthorizationBackend(log *zap.Logger, b *APIBackend) *AuthorizationBackend {
	return &AuthorizationBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		AuthorizationService: b.AuthorizationService,
		OrganizationService:  b.OrganizationService,
		UserService:          b.UserService,
		LookupService:        b.LookupService,
	}
}

// AuthorizationHandler represents an HTTP API handler for authorizations.
type AuthorizationHandler struct {
	*httprouter.Router
	errors2.HTTPErrorHandler
	log *zap.Logger

	OrganizationService  influxdb.OrganizationService
	UserService          influxdb.UserService
	AuthorizationService influxdb.AuthorizationService
	LookupService        influxdb.LookupService
}

// NewAuthorizationHandler returns a new instance of AuthorizationHandler.
func NewAuthorizationHandler(log *zap.Logger, b *AuthorizationBackend) *AuthorizationHandler {
	h := &AuthorizationHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		AuthorizationService: b.AuthorizationService,
		OrganizationService:  b.OrganizationService,
		UserService:          b.UserService,
		LookupService:        b.LookupService,
	}

	h.HandlerFunc("POST", "/api/v2/authorizations", h.handlePostAuthorization)
	h.HandlerFunc("GET", "/api/v2/authorizations", h.handleGetAuthorizations)
	h.HandlerFunc("GET", "/api/v2/authorizations/:id", h.handleGetAuthorization)
	h.HandlerFunc("PATCH", "/api/v2/authorizations/:id", h.handleUpdateAuthorization)
	h.HandlerFunc("DELETE", "/api/v2/authorizations/:id", h.handleDeleteAuthorization)
	return h
}

type authResponse struct {
	ID          platform.ID          `json:"id"`
	Token       string               `json:"token"`
	Status      influxdb.Status      `json:"status"`
	Description string               `json:"description"`
	OrgID       platform.ID          `json:"orgID"`
	Org         string               `json:"org"`
	UserID      platform.ID          `json:"userID"`
	User        string               `json:"user"`
	Permissions []permissionResponse `json:"permissions"`
	Links       map[string]string    `json:"links"`
	CreatedAt   time.Time            `json:"createdAt"`
	UpdatedAt   time.Time            `json:"updatedAt"`
}

func newAuthResponse(a *influxdb.Authorization, org *influxdb.Organization, user *influxdb.User, ps []permissionResponse) *authResponse {
	res := &authResponse{
		ID:          a.ID,
		Token:       a.Token,
		Status:      a.Status,
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

func newPermissionsResponse(ctx context.Context, ps []influxdb.Permission, svc influxdb.LookupService) ([]permissionResponse, error) {
	res := make([]permissionResponse, len(ps))
	for i, p := range ps {
		res[i] = permissionResponse{
			Action: p.Action,
			Resource: resourceResponse{
				Resource: p.Resource,
			},
		}

		if p.Resource.ID != nil {
			name, err := svc.FindResourceName(ctx, p.Resource.Type, *p.Resource.ID)
			if errors2.ErrorCode(err) == errors2.ENotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			res[i].Resource.Name = name
		}

		if p.Resource.OrgID != nil {
			name, err := svc.FindResourceName(ctx, influxdb.OrgsResourceType, *p.Resource.OrgID)
			if errors2.ErrorCode(err) == errors2.ENotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			res[i].Resource.Organization = name
		}
	}
	return res, nil
}

type authsResponse struct {
	Links map[string]string `json:"links"`
	Auths []*authResponse   `json:"authorizations"`
}

func newAuthsResponse(as []*authResponse) *authsResponse {
	return &authsResponse{
		// TODO(desa): update links to include paging and filter information
		Links: map[string]string{
			"self": "/api/v2/authorizations",
		},
		Auths: as,
	}
}

// handlePostAuthorization is the HTTP handler for the POST /api/v2/authorizations route.
func (h *AuthorizationHandler) handlePostAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostAuthorizationRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	user, err := getAuthorizedUser(r, h.UserService)
	if err != nil {
		h.HandleHTTPError(ctx, influxdb.ErrUnableToCreateToken, w)
		return
	}

	userID := user.ID
	if req.UserID != nil && req.UserID.Valid() {
		userID = *req.UserID
	}

	auth := req.toPlatform(userID)

	org, err := h.OrganizationService.FindOrganizationByID(ctx, auth.OrgID)
	if err != nil {
		h.HandleHTTPError(ctx, influxdb.ErrUnableToCreateToken, w)
		return
	}

	if err := h.AuthorizationService.CreateAuthorization(ctx, auth); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	perms, err := newPermissionsResponse(ctx, auth.Permissions, h.LookupService)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Auth created ", zap.String("auth", fmt.Sprint(auth)))

	if err := encodeResponse(ctx, w, http.StatusCreated, newAuthResponse(auth, org, user, perms)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type postAuthorizationRequest struct {
	Status      influxdb.Status       `json:"status"`
	OrgID       platform.ID           `json:"orgID"`
	UserID      *platform.ID          `json:"userID,omitempty"`
	Description string                `json:"description"`
	Permissions []influxdb.Permission `json:"permissions"`
}

func (p *postAuthorizationRequest) toPlatform(userID platform.ID) *influxdb.Authorization {
	return &influxdb.Authorization{
		OrgID:       p.OrgID,
		Status:      p.Status,
		Description: p.Description,
		Permissions: p.Permissions,
		UserID:      userID,
	}
}

func newPostAuthorizationRequest(a *influxdb.Authorization) (*postAuthorizationRequest, error) {
	res := &postAuthorizationRequest{
		OrgID:       a.OrgID,
		Description: a.Description,
		Permissions: a.Permissions,
		Status:      a.Status,
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
}

func (p *postAuthorizationRequest) Validate() error {
	if len(p.Permissions) == 0 {
		return &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "authorization must include permissions",
		}
	}

	for _, perm := range p.Permissions {
		if err := perm.Valid(); err != nil {
			return &errors2.Error{
				Err: err,
			}
		}
	}

	if !p.OrgID.Valid() {
		return &errors2.Error{
			Err:  platform.ErrInvalidID,
			Code: errors2.EInvalid,
			Msg:  "org id required",
		}
	}

	if p.Status == "" {
		p.Status = influxdb.Active
	}

	err := p.Status.Valid()
	if err != nil {
		return err
	}

	return nil
}

func decodePostAuthorizationRequest(ctx context.Context, r *http.Request) (*postAuthorizationRequest, error) {
	a := &postAuthorizationRequest{}
	if err := json.NewDecoder(r.Body).Decode(a); err != nil {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "invalid json structure",
			Err:  err,
		}
	}

	a.SetDefaults()

	return a, a.Validate()
}

// handleGetAuthorizations is the HTTP handler for the GET /api/v2/authorizations route.
func (h *AuthorizationHandler) handleGetAuthorizations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetAuthorizationsRequest(ctx, r)
	if err != nil {
		h.log.Info("Failed to decode request", zap.String("handler", "getAuthorizations"), zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	opts := influxdb.FindOptions{}
	as, _, err := h.AuthorizationService.FindAuthorizations(ctx, req.filter, opts)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auths := make([]*authResponse, 0, len(as))
	for _, a := range as {
		o, err := h.OrganizationService.FindOrganizationByID(ctx, a.OrgID)
		if err != nil {
			h.log.Info("Failed to get organization", zap.String("handler", "getAuthorizations"), zap.String("orgID", a.OrgID.String()), zap.Error(err))
			continue
		}

		u, err := h.UserService.FindUserByID(ctx, a.UserID)
		if err != nil {
			h.log.Info("Failed to get user", zap.String("handler", "getAuthorizations"), zap.String("userID", a.UserID.String()), zap.Error(err))
			continue
		}

		ps, err := newPermissionsResponse(ctx, a.Permissions, h.LookupService)
		if err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}

		auths = append(auths, newAuthResponse(a, o, u, ps))
	}

	h.log.Debug("Auths retrieved ", zap.String("auths", fmt.Sprint(auths)))

	if err := encodeResponse(ctx, w, http.StatusOK, newAuthsResponse(auths)); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

type getAuthorizationsRequest struct {
	filter influxdb.AuthorizationFilter
}

func decodeGetAuthorizationsRequest(ctx context.Context, r *http.Request) (*getAuthorizationsRequest, error) {
	qp := r.URL.Query()

	req := &getAuthorizationsRequest{}

	userID := qp.Get("userID")
	if userID != "" {
		id, err := platform.IDFromString(userID)
		if err != nil {
			return nil, err
		}
		req.filter.UserID = id
	}

	user := qp.Get("user")
	if user != "" {
		req.filter.User = &user
	}

	orgID := qp.Get("orgID")
	if orgID != "" {
		id, err := platform.IDFromString(orgID)
		if err != nil {
			return nil, err
		}
		req.filter.OrgID = id
	}

	org := qp.Get("org")
	if org != "" {
		req.filter.Org = &org
	}

	authID := qp.Get("id")
	if authID != "" {
		id, err := platform.IDFromString(authID)
		if err != nil {
			return nil, err
		}
		req.filter.ID = id
	}

	return req, nil
}

// handleGetAuthorization is the HTTP handler for the GET /api/v2/authorizations/:id route.
func (h *AuthorizationHandler) handleGetAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetAuthorizationRequest(ctx, r)
	if err != nil {
		h.log.Info("Failed to decode request", zap.String("handler", "getAuthorization"), zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	a, err := h.AuthorizationService.FindAuthorizationByID(ctx, req.ID)
	if err != nil {
		// Don't log here, it should already be handled by the service
		h.HandleHTTPError(ctx, err, w)
		return
	}

	o, err := h.OrganizationService.FindOrganizationByID(ctx, a.OrgID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	u, err := h.UserService.FindUserByID(ctx, a.UserID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	ps, err := newPermissionsResponse(ctx, a.Permissions, h.LookupService)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Auth retrieved ", zap.String("auth", fmt.Sprint(a)))

	if err := encodeResponse(ctx, w, http.StatusOK, newAuthResponse(a, o, u, ps)); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

type getAuthorizationRequest struct {
	ID platform.ID
}

func decodeGetAuthorizationRequest(ctx context.Context, r *http.Request) (*getAuthorizationRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &getAuthorizationRequest{
		ID: i,
	}, nil
}

// handleUpdateAuthorization is the HTTP handler for the PATCH /api/v2/authorizations/:id route that updates the authorization's status and desc.
func (h *AuthorizationHandler) handleUpdateAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeUpdateAuthorizationRequest(ctx, r)
	if err != nil {
		h.log.Info("Failed to decode request", zap.String("handler", "updateAuthorization"), zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	a, err := h.AuthorizationService.FindAuthorizationByID(ctx, req.ID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	a, err = h.AuthorizationService.UpdateAuthorization(ctx, a.ID, req.AuthorizationUpdate)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	o, err := h.OrganizationService.FindOrganizationByID(ctx, a.OrgID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	u, err := h.UserService.FindUserByID(ctx, a.UserID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	ps, err := newPermissionsResponse(ctx, a.Permissions, h.LookupService)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Auth updated", zap.String("auth", fmt.Sprint(a)))

	if err := encodeResponse(ctx, w, http.StatusOK, newAuthResponse(a, o, u, ps)); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

type updateAuthorizationRequest struct {
	ID platform.ID
	*influxdb.AuthorizationUpdate
}

func decodeUpdateAuthorizationRequest(ctx context.Context, r *http.Request) (*updateAuthorizationRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	upd := &influxdb.AuthorizationUpdate{}
	if err := json.NewDecoder(r.Body).Decode(upd); err != nil {
		return nil, err
	}

	return &updateAuthorizationRequest{
		ID:                  i,
		AuthorizationUpdate: upd,
	}, nil
}

// handleDeleteAuthorization is the HTTP handler for the DELETE /api/v2/authorizations/:id route.
func (h *AuthorizationHandler) handleDeleteAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeDeleteAuthorizationRequest(ctx, r)
	if err != nil {
		h.log.Info("Failed to decode request", zap.String("handler", "deleteAuthorization"), zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.AuthorizationService.DeleteAuthorization(ctx, req.ID); err != nil {
		// Don't log here, it should already be handled by the service
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Auth deleted", zap.String("authID", fmt.Sprint(req.ID)))

	w.WriteHeader(http.StatusNoContent)
}

type deleteAuthorizationRequest struct {
	ID platform.ID
}

func decodeDeleteAuthorizationRequest(ctx context.Context, r *http.Request) (*deleteAuthorizationRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteAuthorizationRequest{
		ID: i,
	}, nil
}

func getAuthorizedUser(r *http.Request, svc influxdb.UserService) (*influxdb.User, error) {
	ctx := r.Context()

	a, err := platcontext.GetAuthorizer(ctx)
	if err != nil {
		return nil, err
	}

	return svc.FindUserByID(ctx, a.GetUserID())
}

// AuthorizationService connects to Influx via HTTP using tokens to manage authorizations
type AuthorizationService struct {
	Client *httpc.Client
}

var _ influxdb.AuthorizationService = (*AuthorizationService)(nil)

// FindAuthorizationByID finds the authorization against a remote influx server.
func (s *AuthorizationService) FindAuthorizationByID(ctx context.Context, id platform.ID) (*influxdb.Authorization, error) {
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
func (s *AuthorizationService) FindAuthorizationByToken(ctx context.Context, token string) (*influxdb.Authorization, error) {
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
func (s *AuthorizationService) UpdateAuthorization(ctx context.Context, id platform.ID, upd *influxdb.AuthorizationUpdate) (*influxdb.Authorization, error) {
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
func (s *AuthorizationService) DeleteAuthorization(ctx context.Context, id platform.ID) error {
	return s.Client.
		Delete(prefixAuthorization, id.String()).
		Do(ctx)
}
