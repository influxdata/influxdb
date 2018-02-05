package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/oauth2"
)

func (s *Service) mapPrincipalToRoles(ctx context.Context, p oauth2.Principal) ([]chronograf.Role, error) {
	mappings, err := s.Store.Mappings(ctx).All(ctx)
	if err != nil {
		return nil, err
	}
	roles := []chronograf.Role{}
MappingsLoop:
	for _, mapping := range mappings {
		if applyMapping(mapping, p) {
			org, err := s.Store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &mapping.Organization})
			if err != nil {
				continue MappingsLoop
			}

			for _, role := range roles {
				if role.Organization == org.ID {
					continue MappingsLoop
				}
			}
			roles = append(roles, chronograf.Role{Organization: org.ID, Name: org.DefaultRole})
		}
	}

	return roles, nil
}

func applyMapping(m chronograf.Mapping, p oauth2.Principal) bool {
	switch m.Provider {
	case chronograf.MappingWildcard, p.Issuer:
	default:
		return false
	}

	switch m.Scheme {
	case chronograf.MappingWildcard, "oauth2":
	default:
		return false
	}

	if m.Group == chronograf.MappingWildcard {
		return true
	}

	groups := strings.Split(p.Group, ",")

	return matchGroup(m.Group, groups)
}

func matchGroup(match string, groups []string) bool {
	for _, group := range groups {
		if match == group {
			return true
		}
	}

	return false
}

type mappingsRequest chronograf.Mapping

// Valid determines if a mapping request is valid
func (m *mappingsRequest) Valid() error {
	if m.Provider == "" {
		return fmt.Errorf("mapping must specify provider")
	}
	if m.Scheme == "" {
		return fmt.Errorf("mapping must specify scheme")
	}
	if m.Group == "" {
		return fmt.Errorf("mapping must specify group")
	}

	return nil
}

type mappingResponse struct {
	Links selfLinks `json:"links"`
	*chronograf.Mapping
}

func newMappingResponse(m *chronograf.Mapping) *mappingResponse {
	id := "unknown"
	if m != nil {
		id = m.ID
	}
	return &mappingResponse{
		Links: selfLinks{
			Self: fmt.Sprintf("/chronograf/v1/mappings/%s", id),
		},
		Mapping: m,
	}
}

type mappingsResponse struct {
	Links    selfLinks          `json:"links"`
	Mappings []*mappingResponse `json:"mappings"`
}

func newMappingsResponse(ms []chronograf.Mapping) *mappingsResponse {
	mappings := []*mappingResponse{}
	for _, m := range ms {
		mappings = append(mappings, newMappingResponse(&m))
	}
	return &mappingsResponse{
		Links: selfLinks{
			Self: "/chronograf/v1/mappings",
		},
		Mappings: mappings,
	}
}

// Mappings retrives all mappings
func (s *Service) Mappings(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	mappings, err := s.Store.Mappings(ctx).All(ctx)
	if err != nil {
		Error(w, http.StatusInternalServerError, "failed to retrieve mappings from database", s.Logger)
		return
	}

	res := newMappingsResponse(mappings)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// NewMapping adds a new mapping
func (s *Service) NewMapping(w http.ResponseWriter, r *http.Request) {
	var req mappingsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err := req.Valid(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	ctx := r.Context()

	// validate that the organization exists
	if !s.organizationExists(ctx, req.Organization) {
		invalidData(w, fmt.Errorf("organization does not exist"), s.Logger)
		return
	}

	mapping := &chronograf.Mapping{
		Organization: req.Organization,
		Scheme:       req.Scheme,
		Provider:     req.Provider,
		Group:        req.Group,
	}

	m, err := s.Store.Mappings(ctx).Add(ctx, mapping)
	if err != nil {
		Error(w, http.StatusInternalServerError, "failed to add mapping to database", s.Logger)
		return
	}

	cu := newMappingResponse(m)
	location(w, cu.Links.Self)
	encodeJSON(w, http.StatusCreated, cu, s.Logger)
}

// UpdateMapping updates a mapping
func (s *Service) UpdateMapping(w http.ResponseWriter, r *http.Request) {
	var req mappingsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err := req.Valid(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	ctx := r.Context()

	// validate that the organization exists
	if !s.organizationExists(ctx, req.Organization) {
		invalidData(w, fmt.Errorf("organization does not exist"), s.Logger)
		return
	}

	mapping := &chronograf.Mapping{
		ID:           req.ID,
		Organization: req.Organization,
		Scheme:       req.Scheme,
		Provider:     req.Provider,
		Group:        req.Group,
	}

	err := s.Store.Mappings(ctx).Update(ctx, mapping)
	if err != nil {
		Error(w, http.StatusInternalServerError, "failed to update mapping in database", s.Logger)
		return
	}

	cu := newMappingResponse(mapping)
	location(w, cu.Links.Self)
	encodeJSON(w, http.StatusOK, cu, s.Logger)
}

// RemoveMapping removes a mapping
func (s *Service) RemoveMapping(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := httprouter.GetParamFromContext(ctx, "id")

	m, err := s.Store.Mappings(ctx).Get(ctx, id)
	if err == chronograf.ErrMappingNotFound {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
		return
	}

	if err != nil {
		Error(w, http.StatusInternalServerError, "failed to retrieve mapping from database", s.Logger)
		return
	}

	if err := s.Store.Mappings(ctx).Delete(ctx, m); err != nil {
		Error(w, http.StatusInternalServerError, "failed to remove mapping from database", s.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) organizationExists(ctx context.Context, orgID string) bool {
	if _, err := s.Store.Organizations(ctx).Get(ctx, chronograf.OrganizationQuery{ID: &orgID}); err != nil {
		return false
	}

	return true
}
