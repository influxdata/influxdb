package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/roles"
)

func parseOrganizationID(id string) (uint64, error) {
	return strconv.ParseUint(id, 10, 64)
}

type organizationRequest struct {
	Name        string `json:"name"`
	DefaultRole string `json:"defaultRole"`
}

func (r *organizationRequest) ValidCreate() error {
	if r.Name == "" {
		return fmt.Errorf("Name required on Chronograf Organization request body")
	}

	return r.ValidDefaultRole()
}

func (r *organizationRequest) ValidUpdate() error {
	if r.Name == "" && r.DefaultRole == "" {
		return fmt.Errorf("No fields to update")
	}
	return r.ValidDefaultRole()
}

func (r *organizationRequest) ValidDefaultRole() error {
	if r.DefaultRole == "" {
		r.DefaultRole = roles.MemberRoleName
	}

	switch r.DefaultRole {
	case roles.MemberRoleName, roles.ViewerRoleName, roles.EditorRoleName, roles.AdminRoleName:
		return nil
	default:
		return fmt.Errorf("default role must be member, viewer, editor, or admin")
	}
}

type organizationResponse struct {
	Links       selfLinks `json:"links"`
	ID          uint64    `json:"id,string"`
	Name        string    `json:"name"`
	DefaultRole string    `json:"defaultRole,omitempty"`
}

func newOrganizationResponse(o *chronograf.Organization) *organizationResponse {
	return &organizationResponse{
		ID:          o.ID,
		Name:        o.Name,
		DefaultRole: o.DefaultRole,
		Links: selfLinks{
			Self: fmt.Sprintf("/chronograf/v1/organizations/%d", o.ID),
		},
	}
}

type organizationsResponse struct {
	Links         selfLinks               `json:"links"`
	Organizations []*organizationResponse `json:"organizations"`
}

func newOrganizationsResponse(orgs []chronograf.Organization) *organizationsResponse {
	orgsResp := make([]*organizationResponse, len(orgs))
	for i, org := range orgs {
		orgsResp[i] = newOrganizationResponse(&org)
	}
	return &organizationsResponse{
		Organizations: orgsResp,
		Links: selfLinks{
			Self: "/chronograf/v1/organizations",
		},
	}
}

// Organizations retrieves all organizations from store
func (s *Service) Organizations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	orgs, err := s.Store.Organizations(ctx).All(ctx)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	res := newOrganizationsResponse(orgs)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// NewOrganization adds a new organization to store
func (s *Service) NewOrganization(w http.ResponseWriter, r *http.Request) {
	var req organizationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err := req.ValidCreate(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	ctx := r.Context()
	org := &chronograf.Organization{
		Name:        req.Name,
		DefaultRole: req.DefaultRole,
	}

	res, err := s.Store.Organizations(ctx).Add(ctx, org)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	co := newOrganizationResponse(res)
	location(w, co.Links.Self)
	encodeJSON(w, http.StatusCreated, co, s.Logger)
}

// OrganizationID retrieves a organization with ID from store
func (s *Service) OrganizationID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	idStr := httprouter.GetParamFromContext(ctx, "id")
	id, err := parseOrganizationID(idStr)
	if err != nil {
		Error(w, http.StatusBadRequest, fmt.Sprintf("invalid organization id: %s", err.Error()), s.Logger)
		return
	}

	org, err := s.Store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &id})
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	res := newOrganizationResponse(org)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// UpdateOrganization updates an organization in the organizations store
func (s *Service) UpdateOrganization(w http.ResponseWriter, r *http.Request) {
	var req organizationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err := req.ValidUpdate(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	ctx := r.Context()
	idStr := httprouter.GetParamFromContext(ctx, "id")
	id, err := parseOrganizationID(idStr)
	if err != nil {
		Error(w, http.StatusBadRequest, fmt.Sprintf("invalid organization id: %s", err.Error()), s.Logger)
		return
	}

	org, err := s.Store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &id})
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	if req.Name != "" {
		org.Name = req.Name
	}

	err = s.Store.Organizations(ctx).Update(ctx, org)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}

	res := newOrganizationResponse(org)
	location(w, res.Links.Self)
	encodeJSON(w, http.StatusOK, res, s.Logger)

}

// RemoveOrganization removes an organization in the organizations store
func (s *Service) RemoveOrganization(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	idStr := httprouter.GetParamFromContext(ctx, "id")
	id, err := parseOrganizationID(idStr)
	if err != nil {
		Error(w, http.StatusBadRequest, fmt.Sprintf("invalid organization id: %s", err.Error()), s.Logger)
		return
	}

	org, err := s.Store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &id})
	if err != nil {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
		return
	}
	if err := s.Store.Organizations(ctx).Delete(ctx, org); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
