package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/kit/errors"
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
	return h
}

// handlePostOrg is the HTTP handler for the POST /v1/orgs route.
func (h *OrgHandler) handlePostOrg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostOrgRequest(ctx, r)
	if err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := h.OrganizationService.CreateOrganization(ctx, req.Org); err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, req.Org); err != nil {
		errors.EncodeHTTP(ctx, err, w)
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
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	b, err := h.OrganizationService.FindOrganizationByID(ctx, req.OrgID)
	if err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, b); err != nil {
		errors.EncodeHTTP(ctx, err, w)
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
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := (&i).Decode([]byte(id)); err != nil {
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
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	orgs, _, err := h.OrganizationService.FindOrganizations(ctx, req.filter)
	if err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, orgs); err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}
}

type getOrgsRequest struct {
	filter platform.OrganizationFilter
}

func decodeGetOrgsRequest(ctx context.Context, r *http.Request) (*getOrgsRequest, error) {
	qp := r.URL.Query()
	req := &getOrgsRequest{}

	if id := qp.Get("orgID"); id != "" {
		req.filter.ID = &platform.ID{}
		if err := req.filter.ID.DecodeFromString(id); err != nil {
			return nil, err
		}
	}

	if name := qp.Get("orgName"); name != "" {
		req.filter.Name = &name
	}

	return req, nil
}

// handlePatchOrg is the HTTP handler for the PATH /v1/orgs route.
func (h *OrgHandler) handlePatchOrg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchOrgRequest(ctx, r)
	if err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	o, err := h.OrganizationService.UpdateOrganization(ctx, req.OrgID, req.Update)
	if err != nil {
		errors.EncodeHTTP(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, o); err != nil {
		errors.EncodeHTTP(ctx, err, w)
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
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := (&i).Decode([]byte(id)); err != nil {
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
		return nil, fmt.Errorf("expected at least one organization")
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
		qp.Add("orgName", *filter.Name)
	}
	if filter.ID != nil {
		qp.Add("orgID", filter.ID.String())
	}
	url.RawQuery = qp.Encode()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	req.Header.Set("Authorization", s.Token)
	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}

	// TODO: this should really check the error from the headers
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		dec := json.NewDecoder(resp.Body)
		var reqErr errors.Error
		if err := dec.Decode(&reqErr); err != nil {
			return nil, 0, err
		}
		return nil, 0, reqErr
	}

	var os []*platform.Organization

	if err := json.NewDecoder(resp.Body).Decode(&os); err != nil {
		return nil, 0, err
	}

	return os, len(os), nil

}

// CreateOrganization creates an organization.
func (s *OrganizationService) CreateOrganization(ctx context.Context, o *platform.Organization) error {
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
	req.Header.Set("Authorization", s.Token)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	// TODO: this should really check the error from the headers
	if resp.StatusCode != http.StatusCreated {
		defer resp.Body.Close()
		dec := json.NewDecoder(resp.Body)
		var reqErr errors.Error
		if err := dec.Decode(&reqErr); err != nil {
			return err
		}
		return reqErr
	}

	if err := json.NewDecoder(resp.Body).Decode(o); err != nil {
		return err
	}

	return nil
}

func (s *OrganizationService) UpdateOrganization(ctx context.Context, id platform.ID, upd platform.OrganizationUpdate) (*platform.Organization, error) {
	panic("not implemented")
}

func (s *OrganizationService) DeleteOrganization(ctx context.Context, id platform.ID) error {
	panic("not implemented")
}
