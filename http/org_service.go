package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// OrgHandler represents an HTTP API handler for orgs.
type OrgHandler struct {
	*httprouter.Router

	OrganizationService platform.OrganizationService
}

// NewOrgHandler returns a new instance of OrgHandler.
func NewOrgHandler() *OrgHandler {
	h := &OrgHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", "/v1/orgs", h.handlePostOrg)
	h.HandlerFunc("GET", "/v1/orgs", h.handleGetOrgs)
	h.HandlerFunc("GET", "/v1/orgs/:id", h.handleGetOrg)
	h.HandlerFunc("PATCH", "/v1/orgs/:id", h.handlePatchOrg)
	h.HandlerFunc("DELETE", "/v1/orgs/:id", h.handleDeleteOrg)
	return h
}

// handlePostOrg is the HTTP handler for the POST /v1/orgs route.
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

	if err := encodeResponse(ctx, w, http.StatusCreated, req.Org); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type postOrgRequest struct {
	Org *platform.Organization
}

func decodePostOrgRequest(ctx context.Context, r *http.Request) (*postOrgRequest, error) {
	o := &platform.Organization{}
	if err := json.NewDecoder(r.Body).Decode(o); err != nil {
		return nil, err
	}

	return &postOrgRequest{
		Org: o,
	}, nil
}

// handleGetOrg is the HTTP handler for the GET /v1/orgs/:id route.
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

	if err := encodeResponse(ctx, w, http.StatusOK, b); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getOrgRequest struct {
	OrgID platform.ID
}

func decodeGetOrgRequest(ctx context.Context, r *http.Request) (*getOrgRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	req := &getOrgRequest{
		OrgID: i,
	}

	return req, nil
}

// handleGetOrgs is the HTTP handler for the GET /v1/orgs route.
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

	if err := encodeResponse(ctx, w, http.StatusOK, orgs); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getOrgsRequest struct {
	filter platform.OrganizationFilter
}

func decodeGetOrgsRequest(ctx context.Context, r *http.Request) (*getOrgsRequest, error) {
	qp := r.URL.Query()
	req := &getOrgsRequest{}

	if id := qp.Get("id"); id != "" {
		req.filter.ID = &platform.ID{}
		if err := req.filter.ID.DecodeFromString(id); err != nil {
			return nil, err
		}
	}

	if name := qp.Get("name"); name != "" {
		req.filter.Name = &name
	}

	return req, nil
}

// handleDeleteOrganization is the HTTP handler for the DELETE /v1/organizations/:id route.
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

	w.WriteHeader(http.StatusAccepted)
}

type deleteOrganizationRequest struct {
	OrganizationID platform.ID
}

func decodeDeleteOrganizationRequest(ctx context.Context, r *http.Request) (*deleteOrganizationRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req := &deleteOrganizationRequest{
		OrganizationID: i,
	}

	return req, nil
}

// handlePatchOrg is the HTTP handler for the PATH /v1/orgs route.
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

	if err := encodeResponse(ctx, w, http.StatusOK, o); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type patchOrgRequest struct {
	Update platform.OrganizationUpdate
	OrgID  platform.ID
}

func decodePatchOrgRequest(ctx context.Context, r *http.Request) (*patchOrgRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	var upd platform.OrganizationUpdate
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, err
	}

	return &patchOrgRequest{
		Update: upd,
		OrgID:  i,
	}, nil
}

const (
	organizationPath = "/v1/orgs"
)

// OrganizationService connects to Influx via HTTP using tokens to manage organizations.
type OrganizationService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

func (s *OrganizationService) FindOrganizationByID(ctx context.Context, id platform.ID) (*platform.Organization, error) {
	filter := platform.OrganizationFilter{ID: &id}
	return s.FindOrganization(ctx, filter)
}

func (s *OrganizationService) FindOrganization(ctx context.Context, filter platform.OrganizationFilter) (*platform.Organization, error) {
	os, n, err := s.FindOrganizations(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n < 1 {
		return nil, errors.New("expected at least one organization")
	}

	return os[0], nil
}

func (s *OrganizationService) FindOrganizations(ctx context.Context, filter platform.OrganizationFilter, opt ...platform.FindOptions) ([]*platform.Organization, int, error) {
	url, err := newURL(s.Addr, organizationPath)
	if err != nil {
		return nil, 0, err
	}
	qp := url.Query()

	if filter.Name != nil {
		qp.Add("name", *filter.Name)
	}
	if filter.ID != nil {
		qp.Add("id", filter.ID.String())
	}
	url.RawQuery = qp.Encode()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	SetToken(s.Token, req)
	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var os []*platform.Organization
	if err := json.NewDecoder(resp.Body).Decode(&os); err != nil {
		return nil, 0, err
	}

	return os, len(os), nil

}

// CreateOrganization creates an organization.
func (s *OrganizationService) CreateOrganization(ctx context.Context, o *platform.Organization) error {
	if o.Name == "" {
		return kerrors.InvalidDataf("organization name is required")
	}

	url, err := newURL(s.Addr, organizationPath)
	if err != nil {
		return err
	}

	octets, err := json.Marshal(o)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp); err != nil {
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(o); err != nil {
		return err
	}

	return nil
}

func (s *OrganizationService) UpdateOrganization(ctx context.Context, id platform.ID, upd platform.OrganizationUpdate) (*platform.Organization, error) {
	u, err := newURL(s.Addr, organizationIDPath(id))
	if err != nil {
		return nil, err
	}

	octets, err := json.Marshal(upd)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PATCH", u.String(), bytes.NewReader(octets))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var o platform.Organization
	if err := json.NewDecoder(resp.Body).Decode(&o); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &o, nil
}

func (s *OrganizationService) DeleteOrganization(ctx context.Context, id platform.ID) error {
	u, err := newURL(s.Addr, organizationIDPath(id))
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
	return CheckError(resp)
}

func organizationIDPath(id platform.ID) string {
	return path.Join(organizationPath, id.String())
}
