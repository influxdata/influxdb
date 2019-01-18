package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"

	"go.uber.org/zap"

	platform "github.com/influxdata/influxdb"
	platcontext "github.com/influxdata/influxdb/context"
	"github.com/julienschmidt/httprouter"
)

// AuthorizationHandler represents an HTTP API handler for authorizations.
type AuthorizationHandler struct {
	*httprouter.Router
	Logger *zap.Logger

	OrganizationService  platform.OrganizationService
	UserService          platform.UserService
	AuthorizationService platform.AuthorizationService
	LookupService        platform.LookupService
}

// NewAuthorizationHandler returns a new instance of AuthorizationHandler.
func NewAuthorizationHandler(userService platform.UserService) *AuthorizationHandler {
	h := &AuthorizationHandler{
		Router:      NewRouter(),
		Logger:      zap.NewNop(),
		UserService: userService,
	}

	h.HandlerFunc("POST", "/api/v2/authorizations", h.handlePostAuthorization)
	h.HandlerFunc("GET", "/api/v2/authorizations", h.handleGetAuthorizations)
	h.HandlerFunc("GET", "/api/v2/authorizations/:id", h.handleGetAuthorization)
	h.HandlerFunc("PATCH", "/api/v2/authorizations/:id", h.handleSetAuthorizationStatus)
	h.HandlerFunc("DELETE", "/api/v2/authorizations/:id", h.handleDeleteAuthorization)
	return h
}

type authResponse struct {
	ID          platform.ID          `json:"id"`
	Token       string               `json:"token"`
	Status      platform.Status      `json:"status"`
	Description string               `json:"description"`
	OrgID       platform.ID          `json:"orgID"`
	Org         string               `json:"org"`
	UserID      platform.ID          `json:"userID"`
	User        string               `json:"user"`
	Permissions []permissionResponse `json:"permissions"`
	Links       map[string]string    `json:"links"`
}

func newAuthResponse(a *platform.Authorization, org *platform.Organization, user *platform.User, ps []permissionResponse) *authResponse {
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
	}
	return res
}

func (a *authResponse) toPlatform() *platform.Authorization {
	res := &platform.Authorization{
		ID:          a.ID,
		Token:       a.Token,
		Status:      a.Status,
		Description: a.Description,
		OrgID:       a.OrgID,
		UserID:      a.UserID,
	}
	for _, p := range a.Permissions {
		res.Permissions = append(res.Permissions, platform.Permission{Action: p.Action, Resource: p.Resource.Resource})
	}
	return res
}

type permissionResponse struct {
	Action   platform.Action  `json:"action"`
	Resource resourceResponse `json:"resource"`
}

type resourceResponse struct {
	platform.Resource
	Name         string `json:"name,omitempty"`
	Organization string `json:"org,omitempty"`
}

func newPermissionsResponse(ctx context.Context, ps []platform.Permission, svc platform.LookupService) ([]permissionResponse, error) {
	res := make([]permissionResponse, len(ps))
	for i, p := range ps {
		res[i] = permissionResponse{
			Action: p.Action,
			Resource: resourceResponse{
				Resource: p.Resource,
			},
		}

		if p.Resource.ID != nil {
			name, err := svc.Name(ctx, p.Resource.Type, *p.Resource.ID)
			if platform.ErrorCode(err) == platform.ENotFound {
				continue
			}
			if err != nil {
				return nil, err
			}
			res[i].Resource.Name = name
		}

		if p.Resource.OrgID != nil {
			name, err := svc.Name(ctx, platform.OrgsResourceType, *p.Resource.OrgID)
			if platform.ErrorCode(err) == platform.ENotFound {
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
		EncodeError(ctx, err, w)
		return
	}

	var user *platform.User
	// allow the user id to be specified optionally, if it is not set
	// we use the id from the authorizer
	if req.UserID == nil {
		u, err := getAuthorizedUser(r, h.UserService)
		if err != nil {
			EncodeError(ctx, platform.ErrUnableToCreateToken, w)
			return
		}

		user = u
	} else {
		u, err := h.UserService.FindUserByID(ctx, *req.UserID)
		if err != nil {
			EncodeError(ctx, platform.ErrUnableToCreateToken, w)
			return
		}

		user = u
	}

	auth := req.toPlatform(user.ID)

	org, err := h.OrganizationService.FindOrganizationByID(ctx, auth.OrgID)
	if err != nil {
		EncodeError(ctx, platform.ErrUnableToCreateToken, w)
		return
	}

	if err := h.AuthorizationService.CreateAuthorization(ctx, auth); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	perms, err := newPermissionsResponse(ctx, auth.Permissions, h.LookupService)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newAuthResponse(auth, org, user, perms)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type postAuthorizationRequest struct {
	Status      platform.Status       `json:"status"`
	OrgID       platform.ID           `json:"orgID"`
	UserID      *platform.ID          `json:"userID,omitempty"`
	Description string                `json:"description"`
	Permissions []platform.Permission `json:"permissions"`
}

func (p *postAuthorizationRequest) toPlatform(userID platform.ID) *platform.Authorization {
	return &platform.Authorization{
		OrgID:       p.OrgID,
		Status:      p.Status,
		Description: p.Description,
		Permissions: p.Permissions,
		UserID:      userID,
	}
}

func newPostAuthorizationRequest(a *platform.Authorization) (*postAuthorizationRequest, error) {
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
		p.Status = platform.Active
	}
}

func (p *postAuthorizationRequest) Validate() error {
	if len(p.Permissions) == 0 {
		return &platform.Error{
			Code: platform.EInvalid,
			Msg:  "authorization must include permissions",
		}
	}

	for _, perm := range p.Permissions {
		if err := perm.Valid(); err != nil {
			return &platform.Error{
				Err: err,
			}
		}
	}

	if !p.OrgID.Valid() {
		return &platform.Error{
			Err:  platform.ErrInvalidID,
			Code: platform.EInvalid,
			Msg:  "org id required",
		}
	}

	if p.Status == "" {
		p.Status = platform.Active
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
		return nil, &platform.Error{
			Code: platform.EInvalid,
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
		h.Logger.Info("failed to decode request", zap.String("handler", "getAuthorizations"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	opts := platform.FindOptions{}
	as, _, err := h.AuthorizationService.FindAuthorizations(ctx, req.filter, opts)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	auths := make([]*authResponse, len(as))
	for i, a := range as {
		o, err := h.OrganizationService.FindOrganizationByID(ctx, a.OrgID)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		u, err := h.UserService.FindUserByID(ctx, a.UserID)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		ps, err := newPermissionsResponse(ctx, a.Permissions, h.LookupService)
		if err != nil {
			EncodeError(ctx, err, w)
			return
		}

		auths[i] = newAuthResponse(a, o, u, ps)
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newAuthsResponse(auths)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getAuthorizationsRequest struct {
	filter platform.AuthorizationFilter
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
		h.Logger.Info("failed to decode request", zap.String("handler", "getAuthorization"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	a, err := h.AuthorizationService.FindAuthorizationByID(ctx, req.ID)
	if err != nil {
		// Don't log here, it should already be handled by the service
		EncodeError(ctx, err, w)
		return
	}

	o, err := h.OrganizationService.FindOrganizationByID(ctx, a.OrgID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	u, err := h.UserService.FindUserByID(ctx, a.UserID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	ps, err := newPermissionsResponse(ctx, a.Permissions, h.LookupService)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newAuthResponse(a, o, u, ps)); err != nil {
		EncodeError(ctx, err, w)
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
		return nil, &platform.Error{
			Code: platform.EInvalid,
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

// handleSetAuthorizationStatus is the HTTP handler for the PATCH /api/v2/authorizations/:id route that updates the authorization's status.
func (h *AuthorizationHandler) handleSetAuthorizationStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeSetAuthorizationStatusRequest(ctx, r)
	if err != nil {
		h.Logger.Info("failed to decode request", zap.String("handler", "updateAuthorization"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	a, err := h.AuthorizationService.FindAuthorizationByID(ctx, req.ID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if req.Status != a.Status {
		a.Status = req.Status
		if err := h.AuthorizationService.SetAuthorizationStatus(ctx, a.ID, a.Status); err != nil {
			EncodeError(ctx, err, w)
			return
		}
	}

	o, err := h.OrganizationService.FindOrganizationByID(ctx, a.OrgID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	u, err := h.UserService.FindUserByID(ctx, a.UserID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	ps, err := newPermissionsResponse(ctx, a.Permissions, h.LookupService)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newAuthResponse(a, o, u, ps)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type updateAuthorizationRequest struct {
	ID     platform.ID
	Status platform.Status
}

func decodeSetAuthorizationStatusRequest(ctx context.Context, r *http.Request) (*updateAuthorizationRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	a := &setAuthorizationStatusRequest{}
	if err := json.NewDecoder(r.Body).Decode(a); err != nil {
		return nil, err
	}

	return &updateAuthorizationRequest{
		ID:     i,
		Status: a.Status,
	}, nil
}

// handleDeleteAuthorization is the HTTP handler for the DELETE /api/v2/authorizations/:id route.
func (h *AuthorizationHandler) handleDeleteAuthorization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteAuthorizationRequest(ctx, r)
	if err != nil {
		h.Logger.Info("failed to decode request", zap.String("handler", "deleteAuthorization"), zap.Error(err))
		EncodeError(ctx, err, w)
		return
	}

	if err := h.AuthorizationService.DeleteAuthorization(ctx, req.ID); err != nil {
		// Don't log here, it should already be handled by the service
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type deleteAuthorizationRequest struct {
	ID platform.ID
}

func decodeDeleteAuthorizationRequest(ctx context.Context, r *http.Request) (*deleteAuthorizationRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
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

func getAuthorizedUser(r *http.Request, svc platform.UserService) (*platform.User, error) {
	ctx := r.Context()

	a, err := platcontext.GetAuthorizer(ctx)
	if err != nil {
		return nil, err
	}

	return svc.FindUserByID(ctx, a.GetUserID())
}

// AuthorizationService connects to Influx via HTTP using tokens to manage authorizations
type AuthorizationService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

var _ platform.AuthorizationService = (*AuthorizationService)(nil)

// FindAuthorizationByID finds the authorization against a remote influx server.
func (s *AuthorizationService) FindAuthorizationByID(ctx context.Context, id platform.ID) (*platform.Authorization, error) {
	u, err := newURL(s.Addr, authorizationIDPath(id))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp, true); err != nil {
		return nil, err
	}

	var b platform.Authorization
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		return nil, err
	}

	return &b, nil
}

// FindAuthorizationByToken returns a single authorization by Token.
func (s *AuthorizationService) FindAuthorizationByToken(ctx context.Context, token string) (*platform.Authorization, error) {
	return nil, errors.New("not supported in HTTP authorization service")
}

// FindAuthorizations returns a list of authorizations that match filter and the total count of matching authorizations.
// Additional options provide pagination & sorting.
func (s *AuthorizationService) FindAuthorizations(ctx context.Context, filter platform.AuthorizationFilter, opt ...platform.FindOptions) ([]*platform.Authorization, int, error) {
	u, err := newURL(s.Addr, authorizationPath)
	if err != nil {
		return nil, 0, err
	}

	query := u.Query()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	if filter.ID != nil {
		query.Add("id", filter.ID.String())
	}

	if filter.UserID != nil {
		query.Add("userID", filter.UserID.String())
	}

	if filter.User != nil {
		query.Add("user", *filter.User)
	}

	req.URL.RawQuery = query.Encode()
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if err := CheckError(resp, true); err != nil {
		return nil, 0, err
	}

	var as authsResponse
	if err := json.NewDecoder(resp.Body).Decode(&as); err != nil {
		return nil, 0, err
	}

	auths := make([]*platform.Authorization, 0, len(as.Auths))
	for _, a := range as.Auths {
		auths = append(auths, a.toPlatform())
	}

	return auths, len(auths), nil
}

const (
	authorizationPath = "/api/v2/authorizations"
)

// CreateAuthorization creates a new authorization and sets b.ID with the new identifier.
func (s *AuthorizationService) CreateAuthorization(ctx context.Context, a *platform.Authorization) error {
	u, err := newURL(s.Addr, authorizationPath)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}

	newAuth, err := newPostAuthorizationRequest(a)
	if err != nil {
		return err
	}
	octets, err := json.Marshal(newAuth)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp, true); err != nil {
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(a); err != nil {
		return err
	}

	return nil
}

type setAuthorizationStatusRequest struct {
	Status platform.Status `json:"status"`
}

// SetAuthorizationStatus updates an authorization's status.
func (s *AuthorizationService) SetAuthorizationStatus(ctx context.Context, id platform.ID, status platform.Status) error {
	u, err := newURL(s.Addr, authorizationIDPath(id))
	if err != nil {
		return err
	}

	b, err := json.Marshal(setAuthorizationStatusRequest{
		Status: status,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PATCH", u.String(), bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := CheckError(resp, true); err != nil {
		return err
	}

	return nil
}

// DeleteAuthorization removes a authorization by id.
func (s *AuthorizationService) DeleteAuthorization(ctx context.Context, id platform.ID) error {
	u, err := newURL(s.Addr, authorizationIDPath(id))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return CheckError(resp, true)
}

func authorizationIDPath(id platform.ID) string {
	return path.Join(authorizationPath, id.String())
}
