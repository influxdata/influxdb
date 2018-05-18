package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/influxdata/chronograf"
)

type postServiceRequest struct {
	Name               *string `json:"name"`               // User facing name of service instance.; Required: true
	URL                *string `json:"url"`                // URL for the service backend (e.g. http://localhost:9092);/ Required: true
	Type               *string `json:"type"`               // Type is the kind of service (e.g. ifql); Required
	Username           string  `json:"username,omitempty"` // Username for authentication to service
	Password           string  `json:"password,omitempty"`
	InsecureSkipVerify bool    `json:"insecureSkipVerify"` // InsecureSkipVerify as true means any certificate presented by the service is accepted.
	Organization       string  `json:"organization"`       // Organization is the organization ID that resource belongs to

}

func (p *postServiceRequest) Valid(defaultOrgID string) error {
	if p.Name == nil || p.URL == nil {
		return fmt.Errorf("name and url required")
	}

	if p.Type == nil {
		return fmt.Errorf("type required")
	}

	if p.Organization == "" {
		p.Organization = defaultOrgID
	}

	url, err := url.ParseRequestURI(*p.URL)
	if err != nil {
		return fmt.Errorf("invalid source URI: %v", err)
	}
	if len(url.Scheme) == 0 {
		return fmt.Errorf("Invalid URL; no URL scheme defined")
	}

	return nil
}

type serviceLinks struct {
	Proxy  string `json:"proxy"`  // URL location of proxy endpoint for this source
	Self   string `json:"self"`   // Self link mapping to this resource
	Source string `json:"source"` // URL location of the parent source
}

type service struct {
	ID                 int          `json:"id,string"`          // Unique identifier representing a service instance.
	SrcID              int          `json:"sourceID,string"`    // SrcID of the data source
	Name               string       `json:"name"`               // User facing name of service instance.
	URL                string       `json:"url"`                // URL for the service backend (e.g. http://localhost:9092)
	Username           string       `json:"username,omitempty"` // Username for authentication to service
	Password           string       `json:"password,omitempty"`
	InsecureSkipVerify bool         `json:"insecureSkipVerify"` // InsecureSkipVerify as true means any certificate presented by the service is accepted.
	Type               string       `json:"type"`               // Type is the kind of service (e.g. ifql)
	Links              serviceLinks `json:"links"`              // Links are URI locations related to service
}

// NewService adds valid service store store.
func (s *Service) NewService(w http.ResponseWriter, r *http.Request) {
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	_, err = s.Store.Sources(ctx).Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, s.Logger)
		return
	}

	var req postServiceRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	defaultOrg, err := s.Store.Organizations(ctx).DefaultOrganization(ctx)
	if err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}

	if err := req.Valid(defaultOrg.ID); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	srv := chronograf.Server{
		SrcID:              srcID,
		Name:               *req.Name,
		Username:           req.Username,
		Password:           req.Password,
		InsecureSkipVerify: req.InsecureSkipVerify,
		URL:                *req.URL,
		Organization:       req.Organization,
		Type:               *req.Type,
	}

	if srv, err = s.Store.Servers(ctx).Add(ctx, srv); err != nil {
		msg := fmt.Errorf("Error storing service %v: %v", req, err)
		unknownErrorWithMessage(w, msg, s.Logger)
		return
	}

	res := newService(srv)
	location(w, res.Links.Self)
	encodeJSON(w, http.StatusCreated, res, s.Logger)
}

func newService(srv chronograf.Server) service {
	httpAPISrcs := "/chronograf/v1/sources"
	return service{
		ID:                 srv.ID,
		SrcID:              srv.SrcID,
		Name:               srv.Name,
		Username:           srv.Username,
		URL:                srv.URL,
		InsecureSkipVerify: srv.InsecureSkipVerify,
		Type:               srv.Type,
		Links: serviceLinks{
			Self:   fmt.Sprintf("%s/%d/services/%d", httpAPISrcs, srv.SrcID, srv.ID),
			Source: fmt.Sprintf("%s/%d", httpAPISrcs, srv.SrcID),
			Proxy:  fmt.Sprintf("%s/%d/services/%d/proxy", httpAPISrcs, srv.SrcID, srv.ID),
		},
	}
}

type services struct {
	Services []service `json:"services"`
}

// Services retrieves all services from store.
func (s *Service) Services(w http.ResponseWriter, r *http.Request) {
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	mrSrvs, err := s.Store.Servers(ctx).All(ctx)
	if err != nil {
		Error(w, http.StatusInternalServerError, "Error loading services", s.Logger)
		return
	}

	srvs := []service{}
	for _, srv := range mrSrvs {
		if srv.SrcID == srcID && srv.Type != "" {
			srvs = append(srvs, newService(srv))
		}
	}

	res := services{
		Services: srvs,
	}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// ServiceID retrieves a service with ID from store.
func (s *Service) ServiceID(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.Store.Servers(ctx).Get(ctx, id)
	if err != nil || srv.SrcID != srcID || srv.Type == "" {
		notFound(w, id, s.Logger)
		return
	}

	res := newService(srv)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// RemoveService deletes service from store.
func (s *Service) RemoveService(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.Store.Servers(ctx).Get(ctx, id)
	if err != nil || srv.SrcID != srcID || srv.Type == "" {
		notFound(w, id, s.Logger)
		return
	}

	if err = s.Store.Servers(ctx).Delete(ctx, srv); err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type patchServiceRequest struct {
	Name               *string `json:"name,omitempty"`     // User facing name of service instance.
	Type               *string `json:"type,omitempty"`     // Type is the kind of service (e.g. ifql)
	URL                *string `json:"url,omitempty"`      // URL for the service
	Username           *string `json:"username,omitempty"` // Username for service auth
	Password           *string `json:"password,omitempty"`
	InsecureSkipVerify *bool   `json:"insecureSkipVerify"` // InsecureSkipVerify as true means any certificate presented by the service is accepted.
}

func (p *patchServiceRequest) Valid() error {
	if p.URL != nil {
		url, err := url.ParseRequestURI(*p.URL)
		if err != nil {
			return fmt.Errorf("invalid source URI: %v", err)
		}
		if len(url.Scheme) == 0 {
			return fmt.Errorf("Invalid URL; no URL scheme defined")
		}
	}

	if p.Type != nil && *p.Type == "" {
		return fmt.Errorf("Invalid type; type must not be an empty string")
	}

	return nil
}

// UpdateService incrementally updates a service definition in the store
func (s *Service) UpdateService(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	srv, err := s.Store.Servers(ctx).Get(ctx, id)
	if err != nil || srv.SrcID != srcID || srv.Type == "" {
		notFound(w, id, s.Logger)
		return
	}

	var req patchServiceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err := req.Valid(); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	if req.Name != nil {
		srv.Name = *req.Name
	}
	if req.Type != nil {
		srv.Type = *req.Type
	}
	if req.URL != nil {
		srv.URL = *req.URL
	}
	if req.Password != nil {
		srv.Password = *req.Password
	}
	if req.Username != nil {
		srv.Username = *req.Username
	}
	if req.InsecureSkipVerify != nil {
		srv.InsecureSkipVerify = *req.InsecureSkipVerify
	}

	if err := s.Store.Servers(ctx).Update(ctx, srv); err != nil {
		msg := fmt.Sprintf("Error updating service ID %d", id)
		Error(w, http.StatusInternalServerError, msg, s.Logger)
		return
	}

	res := newService(srv)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}
