package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/opentracing/opentracing-go"
	"net/http"
	"path"

	"github.com/influxdata/influxdb"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// OrgBackend is all services and associated parameters required to construct
// the OrgHandler.
type OrgBackend struct {
	Logger *zap.Logger

	OrganizationService             influxdb.OrganizationService
	OrganizationOperationLogService influxdb.OrganizationOperationLogService
	UserResourceMappingService      influxdb.UserResourceMappingService
	SecretService                   influxdb.SecretService
	LabelService                    influxdb.LabelService
	UserService                     influxdb.UserService
}

func NewOrgBackend(b *APIBackend) *OrgBackend {
	return &OrgBackend{
		Logger: b.Logger.With(zap.String("handler", "org")),

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

	Logger *zap.Logger

	OrganizationService             influxdb.OrganizationService
	OrganizationOperationLogService influxdb.OrganizationOperationLogService
	UserResourceMappingService      influxdb.UserResourceMappingService
	SecretService                   influxdb.SecretService
	LabelService                    influxdb.LabelService
	UserService                     influxdb.UserService
}

const (
	organizationsPath            = "/api/v2/orgs"
	organizationsIDPath          = "/api/v2/orgs/:id"
	organizationsIDLogPath       = "/api/v2/orgs/:id/log"
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

// NewOrgHandler returns a new instance of OrgHandler.
func NewOrgHandler(b *OrgBackend) *OrgHandler {
	h := &OrgHandler{
		Router: NewRouter(),
		Logger: zap.NewNop(),

		OrganizationService:             b.OrganizationService,
		OrganizationOperationLogService: b.OrganizationOperationLogService,
		UserResourceMappingService:      b.UserResourceMappingService,
		SecretService:                   b.SecretService,
		LabelService:                    b.LabelService,
		UserService:                     b.UserService,
	}

	h.HandlerFunc("POST", organizationsPath, h.handlePostOrg)
	h.HandlerFunc("GET", organizationsPath, h.handleGetOrgs)
	h.HandlerFunc("GET", organizationsIDPath, h.handleGetOrg)
	h.HandlerFunc("GET", organizationsIDLogPath, h.handleGetOrgLog)
	h.HandlerFunc("PATCH", organizationsIDPath, h.handlePatchOrg)
	h.HandlerFunc("DELETE", organizationsIDPath, h.handleDeleteOrg)

	memberBackend := MemberBackend{
		Logger:                     b.Logger.With(zap.String("handler", "member")),
		ResourceType:               influxdb.OrgsResourceType,
		UserType:                   influxdb.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", organizationsIDMembersPath, newPostMemberHandler(memberBackend))
	h.HandlerFunc("GET", organizationsIDMembersPath, newGetMembersHandler(memberBackend))
	h.HandlerFunc("DELETE", organizationsIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		Logger:                     b.Logger.With(zap.String("handler", "member")),
		ResourceType:               influxdb.OrgsResourceType,
		UserType:                   influxdb.Owner,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", organizationsIDOwnersPath, newPostMemberHandler(ownerBackend))
	h.HandlerFunc("GET", organizationsIDOwnersPath, newGetMembersHandler(ownerBackend))
	h.HandlerFunc("DELETE", organizationsIDOwnersIDPath, newDeleteMemberHandler(ownerBackend))

	h.HandlerFunc("GET", organizationsIDSecretsPath, h.handleGetSecrets)
	h.HandlerFunc("PATCH", organizationsIDSecretsPath, h.handlePatchSecrets)
	// TODO(desa): need a way to specify which secrets to delete. this should work for now
	h.HandlerFunc("POST", organizationsIDSecretsDeletePath, h.handleDeleteSecrets)

	labelBackend := &LabelBackend{
		Logger:       b.Logger.With(zap.String("handler", "label")),
		LabelService: b.LabelService,
		ResourceType: influxdb.OrgsResourceType,
	}
	h.HandlerFunc("GET", organizationsIDLabelsPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", organizationsIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", organizationsIDLabelsIDPath, newDeleteLabelHandler(labelBackend))
	h.HandlerFunc("PATCH", organizationsIDLabelsIDPath, newPatchLabelHandler(labelBackend))

	return h
}

type orgsResponse struct {
	Links         map[string]string `json:"links"`
	Organizations []*orgResponse    `json:"orgs"`
}

func (o orgsResponse) Toinfluxdb() []*influxdb.Organization {
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
		Organizations: []*orgResponse{},
	}
	for _, org := range orgs {
		res.Organizations = append(res.Organizations, newOrgResponse(org))
	}
	return &res
}

type orgResponse struct {
	Links map[string]string `json:"links"`
	influxdb.Organization
}

func newOrgResponse(o *influxdb.Organization) *orgResponse {
	return &orgResponse{
		Links: map[string]string{
			"self":       fmt.Sprintf("/api/v2/orgs/%s", o.ID),
			"log":        fmt.Sprintf("/api/v2/orgs/%s/log", o.ID),
			"members":    fmt.Sprintf("/api/v2/orgs/%s/members", o.ID),
			"secrets":    fmt.Sprintf("/api/v2/orgs/%s/secrets", o.ID),
			"labels":     fmt.Sprintf("/api/v2/orgs/%s/labels", o.ID),
			"buckets":    fmt.Sprintf("/api/v2/buckets?org=%s", o.Name),
			"tasks":      fmt.Sprintf("/api/v2/tasks?org=%s", o.Name),
			"dashboards": fmt.Sprintf("/api/v2/dashboards?org=%s", o.Name),
		},
		Organization: *o,
	}
}

type secretsResponse struct {
	Links   map[string]string `json:"links"`
	Secrets []string          `json:"secrets"`
}

func newSecretsResponse(orgID influxdb.ID, ks []string) *secretsResponse {
	return &secretsResponse{
		Links: map[string]string{
			"org":     fmt.Sprintf("/api/v2/orgs/%s", orgID),
			"secrets": fmt.Sprintf("/api/v2/orgs/%s/secrets", orgID),
		},
		Secrets: ks,
	}
}

// handlePostOrg is the HTTP handler for the POST /api/v2/orgs route.
func (h *OrgHandler) handlePostOrg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostOrgRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.OrganizationService.CreateOrganization(ctx, req.Org); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newOrgResponse(req.Org)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type postOrgRequest struct {
	Org *influxdb.Organization
}

func decodePostOrgRequest(ctx context.Context, r *http.Request) (*postOrgRequest, error) {
	o := &influxdb.Organization{}
	if err := json.NewDecoder(r.Body).Decode(o); err != nil {
		return nil, err
	}

	return &postOrgRequest{
		Org: o,
	}, nil
}

// handleGetOrg is the HTTP handler for the GET /api/v2/orgs/:id route.
func (h *OrgHandler) handleGetOrg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetOrgRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	b, err := h.OrganizationService.FindOrganizationByID(ctx, req.OrgID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newOrgResponse(b)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type getOrgRequest struct {
	OrgID influxdb.ID
}

func decodeGetOrgRequest(ctx context.Context, r *http.Request) (*getOrgRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	req := &getOrgRequest{
		OrgID: i,
	}

	return req, nil
}

// handleGetOrgs is the HTTP handler for the GET /api/v2/orgs route.
func (h *OrgHandler) handleGetOrgs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetOrgsRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	orgs, _, err := h.OrganizationService.FindOrganizations(ctx, req.filter)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newOrgsResponse(orgs)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type getOrgsRequest struct {
	filter influxdb.OrganizationFilter
}

func decodeGetOrgsRequest(ctx context.Context, r *http.Request) (*getOrgsRequest, error) {
	qp := r.URL.Query()
	req := &getOrgsRequest{}

	if orgID := qp.Get(OrgID); orgID != "" {
		id, err := influxdb.IDFromString(orgID)
		if err != nil {
			return nil, err
		}
		req.filter.ID = id
	}

	if name := qp.Get(OrgName); name != "" {
		req.filter.Name = &name
	}

	return req, nil
}

// handleDeleteOrganization is the HTTP handler for the DELETE /api/v2/orgs/:id route.
func (h *OrgHandler) handleDeleteOrg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteOrganizationRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.OrganizationService.DeleteOrganization(ctx, req.OrganizationID); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type deleteOrganizationRequest struct {
	OrganizationID influxdb.ID
}

func decodeDeleteOrganizationRequest(ctx context.Context, r *http.Request) (*deleteOrganizationRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req := &deleteOrganizationRequest{
		OrganizationID: i,
	}

	return req, nil
}

// handlePatchOrg is the HTTP handler for the PATH /api/v2/orgs route.
func (h *OrgHandler) handlePatchOrg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchOrgRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	o, err := h.OrganizationService.UpdateOrganization(ctx, req.OrgID, req.Update)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newOrgResponse(o)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type patchOrgRequest struct {
	Update influxdb.OrganizationUpdate
	OrgID  influxdb.ID
}

func decodePatchOrgRequest(ctx context.Context, r *http.Request) (*patchOrgRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	var upd influxdb.OrganizationUpdate
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, err
	}

	return &patchOrgRequest{
		Update: upd,
		OrgID:  i,
	}, nil
}

// handleGetSecrets is the HTTP handler for the GET /api/v2/orgs/:id/secrets route.
func (h *OrgHandler) handleGetSecrets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetSecretsRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	ks, err := h.SecretService.GetSecretKeys(ctx, req.orgID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newSecretsResponse(req.orgID, ks)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type getSecretsRequest struct {
	orgID influxdb.ID
}

func decodeGetSecretsRequest(ctx context.Context, r *http.Request) (*getSecretsRequest, error) {
	req := &getSecretsRequest{}
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req.orgID = i

	return req, nil
}

// handleGetPatchSecrets is the HTTP handler for the PATCH /api/v2/orgs/:id/secrets route.
func (h *OrgHandler) handlePatchSecrets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchSecretsRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.SecretService.PatchSecrets(ctx, req.orgID, req.secrets); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type patchSecretsRequest struct {
	orgID   influxdb.ID
	secrets map[string]string
}

func decodePatchSecretsRequest(ctx context.Context, r *http.Request) (*patchSecretsRequest, error) {
	req := &patchSecretsRequest{}
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req.orgID = i
	req.secrets = map[string]string{}

	if err := json.NewDecoder(r.Body).Decode(&req.secrets); err != nil {
		return nil, err
	}

	return req, nil
}

// handleDeleteSecrets is the HTTP handler for the DELETE /api/v2/orgs/:id/secrets route.
func (h *OrgHandler) handleDeleteSecrets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteSecretsRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.SecretService.DeleteSecret(ctx, req.orgID, req.secrets...); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type deleteSecretsRequest struct {
	orgID   influxdb.ID
	secrets []string
}

func decodeDeleteSecretsRequest(ctx context.Context, r *http.Request) (*deleteSecretsRequest, error) {
	req := &deleteSecretsRequest{}
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req.orgID = i
	req.secrets = []string{}

	if err := json.NewDecoder(r.Body).Decode(&req.secrets); err != nil {
		return nil, err
	}

	return req, nil
}

const (
	organizationPath = "/api/v2/orgs"
)

// OrganizationService connects to Influx via HTTP using tokens to manage organizations.
type OrganizationService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
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
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "no filter parameters provided",
		}
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
	span, _ := opentracing.StartSpanFromContext(ctx, "OrganizationService.FindOrganizations")
	defer span.Finish()

	if filter.Name != nil {
		span.LogKV("org", *filter.Name)
	}
	if filter.ID != nil {
		span.LogKV("org-id", *filter.ID)
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

	url, err := newURL(s.Addr, organizationPath)
	if err != nil {
		return nil, 0, tracing.LogError(span, err)
	}
	qp := url.Query()

	if filter.Name != nil {
		qp.Add(OrgName, *filter.Name)
	}
	if filter.ID != nil {
		qp.Add(OrgID, filter.ID.String())
	}
	url.RawQuery = qp.Encode()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, 0, tracing.LogError(span, err)
	}
	tracing.InjectToHTTPRequest(span, req)

	SetToken(s.Token, req)
	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, tracing.LogError(span, err)
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, 0, tracing.LogError(span, err)
	}

	var os orgsResponse
	if err := json.NewDecoder(resp.Body).Decode(&os); err != nil {
		return nil, 0, tracing.LogError(span, err)
	}

	orgs := os.Toinfluxdb()
	return orgs, len(orgs), nil
}

// CreateOrganization creates an organization.
func (s *OrganizationService) CreateOrganization(ctx context.Context, o *influxdb.Organization) error {
	if o.Name == "" {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "organization name is required",
		}
	}

	span, _ := opentracing.StartSpanFromContext(ctx, "OrganizationService.CreateOrganization")
	defer span.Finish()

	if o.Name != "" {
		span.LogKV("org", o.Name)
	}
	if o.ID != 0 {
		span.LogKV("org-id", o.ID)
	}

	url, err := newURL(s.Addr, organizationPath)
	if err != nil {
		return tracing.LogError(span, err)
	}

	octets, err := json.Marshal(o)
	if err != nil {
		return tracing.LogError(span, err)
	}

	req, err := http.NewRequest("POST", url.String(), bytes.NewReader(octets))
	if err != nil {
		return tracing.LogError(span, err)
	}
	tracing.InjectToHTTPRequest(span, req)

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return tracing.LogError(span, err)
	}
	defer resp.Body.Close()

	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp); err != nil {
		return tracing.LogError(span, err)
	}

	if err := json.NewDecoder(resp.Body).Decode(o); err != nil {
		return tracing.LogError(span, err)
	}

	return nil
}

// UpdateOrganization updates the organization over HTTP.
func (s *OrganizationService) UpdateOrganization(ctx context.Context, id influxdb.ID, upd influxdb.OrganizationUpdate) (*influxdb.Organization, error) {
	span, _ := opentracing.StartSpanFromContext(ctx, "OrganizationService.UpdateOrganization")
	defer span.Finish()

	span.LogKV("org-id", id)
	span.LogKV("name", upd.Name)

	u, err := newURL(s.Addr, organizationIDPath(id))
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	octets, err := json.Marshal(upd)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	req, err := http.NewRequest("PATCH", u.String(), bytes.NewReader(octets))
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	tracing.InjectToHTTPRequest(span, req)

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return nil, tracing.LogError(span, err)
	}

	var o influxdb.Organization
	if err := json.NewDecoder(resp.Body).Decode(&o); err != nil {
		return nil, tracing.LogError(span, err)
	}

	return &o, nil
}

// DeleteOrganization removes organization id over HTTP.
func (s *OrganizationService) DeleteOrganization(ctx context.Context, id influxdb.ID) error {
	span, _ := opentracing.StartSpanFromContext(ctx, "OrganizationService.DeleteOrganization")
	defer span.Finish()

	u, err := newURL(s.Addr, organizationIDPath(id))
	if err != nil {
		return tracing.LogError(span, err)
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return tracing.LogError(span, err)
	}
	tracing.InjectToHTTPRequest(span, req)

	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return tracing.LogError(span, err)
	}
	defer resp.Body.Close()

	err = CheckErrorStatus(http.StatusNoContent, resp)
	if err != nil {
		return tracing.LogError(span, err)
	}

	return nil
}

func organizationIDPath(id influxdb.ID) string {
	return path.Join(organizationPath, id.String())
}

// hanldeGetOrganizationLog retrieves a organization log by the organizations ID.
func (h *OrgHandler) handleGetOrgLog(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetOrganizationLogRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	log, _, err := h.OrganizationOperationLogService.GetOrganizationOperationLog(ctx, req.OrganizationID, req.opts)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newOrganizationLogResponse(req.OrganizationID, log)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type getOrganizationLogRequest struct {
	OrganizationID influxdb.ID
	opts           influxdb.FindOptions
}

func decodeGetOrganizationLogRequest(ctx context.Context, r *http.Request) (*getOrganizationLogRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	opts, err := decodeFindOptions(ctx, r)
	if err != nil {
		return nil, err
	}

	return &getOrganizationLogRequest{
		OrganizationID: i,
		opts:           *opts,
	}, nil
}

func newOrganizationLogResponse(id influxdb.ID, es []*influxdb.OperationLogEntry) *operationLogResponse {
	log := make([]*operationLogEntryResponse, 0, len(es))
	for _, e := range es {
		log = append(log, newOperationLogEntryResponse(e))
	}
	return &operationLogResponse{
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/orgs/%s/log", id),
		},
		Log: log,
	}
}
