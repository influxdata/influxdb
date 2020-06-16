package http

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"go.uber.org/zap"
)

// OrgBackend is all services and associated parameters required to construct
// the OrgHandler.
type OrgBackend struct {
	influxdb.HTTPErrorHandler
	log *zap.Logger

	OrganizationService             influxdb.OrganizationService
	OrganizationOperationLogService influxdb.OrganizationOperationLogService
	UserResourceMappingService      influxdb.UserResourceMappingService
	SecretService                   influxdb.SecretService
	LabelService                    influxdb.LabelService
	UserService                     influxdb.UserService
}

// NewOrgBackend is a datasource used by the org handler.
func NewOrgBackend(log *zap.Logger, b *APIBackend) *OrgBackend {
	return &OrgBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		OrganizationService:             b.OrganizationService,
		OrganizationOperationLogService: b.OrganizationOperationLogService,
		UserResourceMappingService:      b.UserResourceMappingService,
		SecretService:                   b.SecretService,
		LabelService:                    b.LabelService,
		UserService:                     b.UserService,
	}
}

// OrgHandler represents an HTTP API handler for orgs.
type OrgHandler struct {
	*httprouter.Router
	*kithttp.API
	log *zap.Logger

	OrgSVC                          influxdb.OrganizationService
	OrganizationOperationLogService influxdb.OrganizationOperationLogService
	UserResourceMappingService      influxdb.UserResourceMappingService
	SecretService                   influxdb.SecretService
	LabelService                    influxdb.LabelService
	UserService                     influxdb.UserService
}

const (
	prefixOrganizations          = "/api/v2/orgs"
	organizationsIDPath          = "/api/v2/orgs/:id"
	organizationsIDMembersPath   = "/api/v2/orgs/:id/members"
	organizationsIDMembersIDPath = "/api/v2/orgs/:id/members/:userID"
	organizationsIDOwnersPath    = "/api/v2/orgs/:id/owners"
	organizationsIDOwnersIDPath  = "/api/v2/orgs/:id/owners/:userID"
	organizationsIDSecretsPath   = "/api/v2/orgs/:id/secrets"
	// TODO(desa): need a way to specify which secrets to delete. this should work for now
	organizationsIDSecretsDeletePath = "/api/v2/orgs/:id/secrets/delete"
	organizationsIDLabelsPath        = "/api/v2/orgs/:id/labels"
	organizationsIDLabelsIDPath      = "/api/v2/orgs/:id/labels/:lid"
)

func checkOrganizationExists(orgHandler *OrgHandler) kithttp.Middleware {
	fn := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			orgID, err := decodeIDFromCtx(ctx, "id")
			if err != nil {
				orgHandler.API.Err(w, r, err)
				return
			}

			if _, err := orgHandler.OrgSVC.FindOrganizationByID(ctx, orgID); err != nil {
				orgHandler.API.Err(w, r, err)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	return fn
}

// NewOrgHandler returns a new instance of OrgHandler.
func NewOrgHandler(log *zap.Logger, b *OrgBackend) *OrgHandler {
	h := &OrgHandler{
		Router: NewRouter(b.HTTPErrorHandler),
		API:    kithttp.NewAPI(kithttp.WithLog(log)),
		log:    log,

		OrgSVC:                          b.OrganizationService,
		OrganizationOperationLogService: b.OrganizationOperationLogService,
		UserResourceMappingService:      b.UserResourceMappingService,
		SecretService:                   b.SecretService,
		LabelService:                    b.LabelService,
		UserService:                     b.UserService,
	}

	h.HandlerFunc("POST", prefixOrganizations, h.handlePostOrg)
	h.HandlerFunc("GET", prefixOrganizations, h.handleGetOrgs)
	h.HandlerFunc("GET", organizationsIDPath, h.handleGetOrg)
	h.HandlerFunc("PATCH", organizationsIDPath, h.handlePatchOrg)
	h.HandlerFunc("DELETE", organizationsIDPath, h.handleDeleteOrg)

	memberBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               influxdb.OrgsResourceType,
		UserType:                   influxdb.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", organizationsIDMembersPath, newPostMemberHandler(memberBackend))
	h.Handler("GET", organizationsIDMembersPath, applyMW(newGetMembersHandler(memberBackend), checkOrganizationExists(h)))
	h.HandlerFunc("DELETE", organizationsIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               influxdb.OrgsResourceType,
		UserType:                   influxdb.Owner,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", organizationsIDOwnersPath, newPostMemberHandler(ownerBackend))
	h.Handler("GET", organizationsIDOwnersPath, applyMW(newGetMembersHandler(ownerBackend), checkOrganizationExists(h)))
	h.HandlerFunc("DELETE", organizationsIDOwnersIDPath, newDeleteMemberHandler(ownerBackend))

	h.HandlerFunc("GET", organizationsIDSecretsPath, h.handleGetSecrets)
	h.HandlerFunc("PATCH", organizationsIDSecretsPath, h.handlePatchSecrets)
	// TODO(desa): need a way to specify which secrets to delete. this should work for now
	h.HandlerFunc("POST", organizationsIDSecretsDeletePath, h.handleDeleteSecrets)

	labelBackend := &LabelBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              b.log.With(zap.String("handler", "label")),
		LabelService:     b.LabelService,
		ResourceType:     influxdb.OrgsResourceType,
	}
	h.HandlerFunc("GET", organizationsIDLabelsPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", organizationsIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", organizationsIDLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

type orgsResponse struct {
	Links         map[string]string `json:"links"`
	Organizations []orgResponse     `json:"orgs"`
}

func (o orgsResponse) toInfluxdb() []*influxdb.Organization {
	orgs := make([]*influxdb.Organization, len(o.Organizations))
	for i := range o.Organizations {
		orgs[i] = &o.Organizations[i].Organization
	}
	return orgs
}

func newOrgsResponse(orgs []*influxdb.Organization) *orgsResponse {
	res := orgsResponse{
		Links: map[string]string{
			"self": "/api/v2/orgs",
		},
		Organizations: []orgResponse{},
	}
	for _, org := range orgs {
		res.Organizations = append(res.Organizations, newOrgResponse(*org))
	}
	return &res
}

type orgResponse struct {
	Links map[string]string `json:"links"`
	influxdb.Organization
}

func newOrgResponse(o influxdb.Organization) orgResponse {
	return orgResponse{
		Links: map[string]string{
			"self":       fmt.Sprintf("/api/v2/orgs/%s", o.ID),
			"logs":       fmt.Sprintf("/api/v2/orgs/%s/logs", o.ID),
			"members":    fmt.Sprintf("/api/v2/orgs/%s/members", o.ID),
			"owners":     fmt.Sprintf("/api/v2/orgs/%s/owners", o.ID),
			"secrets":    fmt.Sprintf("/api/v2/orgs/%s/secrets", o.ID),
			"labels":     fmt.Sprintf("/api/v2/orgs/%s/labels", o.ID),
			"buckets":    fmt.Sprintf("/api/v2/buckets?org=%s", o.Name),
			"tasks":      fmt.Sprintf("/api/v2/tasks?org=%s", o.Name),
			"dashboards": fmt.Sprintf("/api/v2/dashboards?org=%s", o.Name),
		},
		Organization: o,
	}
}

// handlePostOrg is the HTTP handler for the POST /api/v2/orgs route.
func (h *OrgHandler) handlePostOrg(w http.ResponseWriter, r *http.Request) {
	var org influxdb.Organization
	if err := h.API.DecodeJSON(r.Body, &org); err != nil {
		h.API.Err(w, r, err)
		return
	}

	if err := h.OrgSVC.CreateOrganization(r.Context(), &org); err != nil {
		h.API.Err(w, r, err)
		return
	}
	h.log.Debug("Org created", zap.String("org", fmt.Sprint(org)))

	h.API.Respond(w, r, http.StatusCreated, newOrgResponse(org))
}

// handleGetOrg is the HTTP handler for the GET /api/v2/orgs/:id route.
func (h *OrgHandler) handleGetOrg(w http.ResponseWriter, r *http.Request) {
	id, err := decodeIDFromCtx(r.Context(), "id")
	if err != nil {
		h.API.Err(w, r, err)
		return
	}

	org, err := h.OrgSVC.FindOrganizationByID(r.Context(), id)
	if err != nil {
		h.API.Err(w, r, err)
		return
	}
	h.log.Debug("Org retrieved", zap.String("org", fmt.Sprint(org)))

	h.API.Respond(w, r, http.StatusOK, newOrgResponse(*org))
}

// handleGetOrgs is the HTTP handler for the GET /api/v2/orgs route.
func (h *OrgHandler) handleGetOrgs(w http.ResponseWriter, r *http.Request) {
	var filter influxdb.OrganizationFilter
	qp := r.URL.Query()
	if name := qp.Get(Org); name != "" {
		filter.Name = &name
	}
	if orgID := qp.Get("orgID"); orgID != "" {
		id, err := influxdb.IDFromString(orgID)
		if err != nil {
			h.API.Err(w, r, err)
			return
		}
		filter.ID = id
	}

	if userID := qp.Get("userID"); userID != "" {
		id, err := influxdb.IDFromString(userID)
		if err != nil {
			h.API.Err(w, r, err)
			return
		}
		filter.UserID = id
	}

	orgs, _, err := h.OrgSVC.FindOrganizations(r.Context(), filter)
	if err != nil {
		h.API.Err(w, r, err)
		return
	}
	h.log.Debug("Orgs retrieved", zap.String("org", fmt.Sprint(orgs)))

	h.API.Respond(w, r, http.StatusOK, newOrgsResponse(orgs))
}

// handleDeleteOrganization is the HTTP handler for the DELETE /api/v2/orgs/:id route.
func (h *OrgHandler) handleDeleteOrg(w http.ResponseWriter, r *http.Request) {
	id, err := decodeIDFromCtx(r.Context(), "id")
	if err != nil {
		h.API.Err(w, r, err)
		return
	}

	ctx := r.Context()
	if err := h.OrgSVC.DeleteOrganization(ctx, id); err != nil {
		h.API.Err(w, r, err)
		return
	}
	h.log.Debug("Org deleted", zap.String("orgID", fmt.Sprint(id)))

	h.API.Respond(w, r, http.StatusNoContent, nil)
}

// handlePatchOrg is the HTTP handler for the PATH /api/v2/orgs route.
func (h *OrgHandler) handlePatchOrg(w http.ResponseWriter, r *http.Request) {
	id, err := decodeIDFromCtx(r.Context(), "id")
	if err != nil {
		h.API.Err(w, r, err)
		return
	}

	var upd influxdb.OrganizationUpdate
	if err := h.API.DecodeJSON(r.Body, &upd); err != nil {
		h.API.Err(w, r, err)
		return
	}

	org, err := h.OrgSVC.UpdateOrganization(r.Context(), id, upd)
	if err != nil {
		h.API.Err(w, r, err)
		return
	}
	h.log.Debug("Org updated", zap.String("org", fmt.Sprint(org)))

	h.API.Respond(w, r, http.StatusOK, newOrgResponse(*org))
}

type secretsResponse struct {
	Links   map[string]string `json:"links"`
	Secrets []string          `json:"secrets"`
}

func newSecretsResponse(orgID influxdb.ID, ks []string) *secretsResponse {
	if ks == nil {
		ks = []string{}
	}
	return &secretsResponse{
		Links: map[string]string{
			"org":  fmt.Sprintf("/api/v2/orgs/%s", orgID),
			"self": fmt.Sprintf("/api/v2/orgs/%s/secrets", orgID),
		},
		Secrets: ks,
	}
}

// handleGetSecrets is the HTTP handler for the GET /api/v2/orgs/:id/secrets route.
func (h *OrgHandler) handleGetSecrets(w http.ResponseWriter, r *http.Request) {
	orgID, err := decodeIDFromCtx(r.Context(), "id")
	if err != nil {
		h.API.Err(w, r, err)
		return
	}

	ks, err := h.SecretService.GetSecretKeys(r.Context(), orgID)
	if err != nil && influxdb.ErrorCode(err) != influxdb.ENotFound {
		h.API.Err(w, r, err)
		return
	}

	h.API.Respond(w, r, http.StatusOK, newSecretsResponse(orgID, ks))
}

// handleGetPatchSecrets is the HTTP handler for the PATCH /api/v2/orgs/:id/secrets route.
func (h *OrgHandler) handlePatchSecrets(w http.ResponseWriter, r *http.Request) {
	orgID, err := decodeIDFromCtx(r.Context(), "id")
	if err != nil {
		h.API.Err(w, r, err)
		return
	}

	var secrets map[string]string
	if err := h.API.DecodeJSON(r.Body, &secrets); err != nil {
		h.API.Err(w, r, err)
		return
	}

	if err := h.SecretService.PatchSecrets(r.Context(), orgID, secrets); err != nil {
		h.API.Err(w, r, err)
		return
	}

	h.API.Respond(w, r, http.StatusNoContent, nil)
}

type secretsDeleteBody struct {
	Secrets []string `json:"secrets"`
}

// handleDeleteSecrets is the HTTP handler for the DELETE /api/v2/orgs/:id/secrets route.
func (h *OrgHandler) handleDeleteSecrets(w http.ResponseWriter, r *http.Request) {
	orgID, err := decodeIDFromCtx(r.Context(), "id")
	if err != nil {
		h.API.Err(w, r, err)
		return
	}

	var reqBody secretsDeleteBody

	if err := h.API.DecodeJSON(r.Body, &reqBody); err != nil {
		h.API.Err(w, r, err)
		return
	}

	if err := h.SecretService.DeleteSecret(r.Context(), orgID, reqBody.Secrets...); err != nil {
		h.API.Err(w, r, err)
		return
	}

	h.API.Respond(w, r, http.StatusNoContent, nil)
}

// SecretService connects to Influx via HTTP using tokens to manage secrets.
type SecretService struct {
	Client *httpc.Client
}

// LoadSecret is not implemented for http
func (s *SecretService) LoadSecret(ctx context.Context, orgID influxdb.ID, k string) (string, error) {
	return "", &influxdb.Error{
		Code: influxdb.EMethodNotAllowed,
		Msg:  "load secret is not implemented for http",
	}
}

// PutSecret is not implemented for http.
func (s *SecretService) PutSecret(ctx context.Context, orgID influxdb.ID, k string, v string) error {
	return &influxdb.Error{
		Code: influxdb.EMethodNotAllowed,
		Msg:  "put secret is not implemented for http",
	}
}

// GetSecretKeys get all secret keys mathing an org ID via HTTP.
func (s *SecretService) GetSecretKeys(ctx context.Context, orgID influxdb.ID) ([]string, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	span.LogKV("org-id", orgID)

	path := strings.Replace(organizationsIDSecretsPath, ":id", orgID.String(), 1)

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
func (s *SecretService) PutSecrets(ctx context.Context, orgID influxdb.ID, m map[string]string) error {
	return &influxdb.Error{
		Code: influxdb.EMethodNotAllowed,
		Msg:  "put secrets is not implemented for http",
	}
}

// PatchSecrets will update the existing secret with new via http.
func (s *SecretService) PatchSecrets(ctx context.Context, orgID influxdb.ID, m map[string]string) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if orgID != 0 {
		span.LogKV("org-id", orgID)
	}

	path := strings.Replace(organizationsIDSecretsPath, ":id", orgID.String(), 1)

	return s.Client.
		PatchJSON(m, path).
		Do(ctx)
}

// DeleteSecret removes a single secret via HTTP.
func (s *SecretService) DeleteSecret(ctx context.Context, orgID influxdb.ID, ks ...string) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	path := strings.Replace(organizationsIDSecretsDeletePath, ":id", orgID.String(), 1)
	return s.Client.
		PostJSON(secretsDeleteBody{
			Secrets: ks,
		}, path).
		Do(ctx)
}

// OrganizationService connects to Influx via HTTP using tokens to manage organizations.
type OrganizationService struct {
	Client *httpc.Client
	// OpPrefix is for not found errors.
	OpPrefix string
}

// FindOrganizationByID gets a single organization with a given id using HTTP.
func (s *OrganizationService) FindOrganizationByID(ctx context.Context, id influxdb.ID) (*influxdb.Organization, error) {
	filter := influxdb.OrganizationFilter{ID: &id}
	o, err := s.FindOrganization(ctx, filter)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
			Op:  s.OpPrefix + influxdb.OpFindOrganizationByID,
		}
	}
	return o, nil
}

// FindOrganization gets a single organization matching the filter using HTTP.
func (s *OrganizationService) FindOrganization(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
	if filter.ID == nil && filter.Name == nil {
		return nil, influxdb.ErrInvalidOrgFilter
	}
	os, n, err := s.FindOrganizations(ctx, filter)
	if err != nil {
		return nil, &influxdb.Error{
			Err: err,
			Op:  s.OpPrefix + influxdb.OpFindOrganization,
		}
	}

	if n == 0 {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Op:   s.OpPrefix + influxdb.OpFindOrganization,
			Msg:  "organization not found",
		}
	}

	return os[0], nil
}

// FindOrganizations returns all organizations that match the filter via HTTP.
func (s *OrganizationService) FindOrganizations(ctx context.Context, filter influxdb.OrganizationFilter, opt ...influxdb.FindOptions) ([]*influxdb.Organization, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	params := influxdb.FindOptionParams(opt...)
	if filter.Name != nil {
		span.LogKV("org", *filter.Name)
		params = append(params, [2]string{"org", *filter.Name})
	}
	if filter.ID != nil {
		span.LogKV("org-id", *filter.ID)
		params = append(params, [2]string{"orgID", filter.ID.String()})
	}
	for _, o := range opt {
		if o.Offset != 0 {
			span.LogKV("offset", o.Offset)
		}
		span.LogKV("descending", o.Descending)
		if o.Limit > 0 {
			span.LogKV("limit", o.Limit)
		}
		if o.SortBy != "" {
			span.LogKV("sortBy", o.SortBy)
		}
	}

	var os orgsResponse
	err := s.Client.
		Get(prefixOrganizations).
		QueryParams(params...).
		DecodeJSON(&os).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	orgs := os.toInfluxdb()
	return orgs, len(orgs), nil
}

// CreateOrganization creates an organization.
func (s *OrganizationService) CreateOrganization(ctx context.Context, o *influxdb.Organization) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if o.Name != "" {
		span.LogKV("org", o.Name)
	}
	if o.ID != 0 {
		span.LogKV("org-id", o.ID)
	}

	return s.Client.
		PostJSON(o, prefixOrganizations).
		DecodeJSON(o).
		Do(ctx)
}

// UpdateOrganization updates the organization over HTTP.
func (s *OrganizationService) UpdateOrganization(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	span.LogKV("org-id", id)
	span.LogKV("name", upd.Name)

	var o influxdb.Organization
	err := s.Client.
		PatchJSON(upd, prefixOrganizations, id.String()).
		DecodeJSON(&o).
		Do(ctx)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	return &o, nil
}

// DeleteOrganization removes organization id over HTTP.
func (s *OrganizationService) DeleteOrganization(ctx context.Context, id influxdb.ID) error {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	return s.Client.
		Delete(prefixOrganizations, id.String()).
		Do(ctx)
}

func organizationIDPath(id influxdb.ID) string {
	return path.Join(prefixOrganizations, id.String())
}
