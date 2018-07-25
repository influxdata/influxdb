package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/bouk/httprouter"
	platform "github.com/influxdata/platform/chronograf/v2"
)

type dashboardV2Links struct {
	Self string `json:"self"`
}

type dashboardV2Response struct {
	platform.Dashboard
	Links dashboardV2Links `json:"links"`
}

func newDashboardV2Response(d *platform.Dashboard) dashboardV2Response {
	// Make nil slice values into empty array for front end.
	if d.Cells == nil {
		d.Cells = []platform.DashboardCell{}
	}
	return dashboardV2Response{
		Links: dashboardV2Links{
			Self: fmt.Sprintf("/chronograf/v2/dashboards/%s", d.ID),
		},
		Dashboard: *d,
	}
}

// DashboardsV2 returns all dashboards within the store.
func (s *Service) DashboardsV2(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// TODO: support filtering via query params
	dashboards, _, err := s.Store.DashboardsV2(ctx).FindDashboards(ctx, platform.DashboardFilter{})
	if err != nil {
		Error(w, http.StatusInternalServerError, "Error loading dashboards", s.Logger)
		return
	}

	s.encodeGetDashboardsResponse(w, dashboards)
}

type getDashboardsV2Links struct {
	Self string `json:"self"`
}

type getDashboardsV2Response struct {
	Links      getDashboardsV2Links  `json:"links"`
	Dashboards []dashboardV2Response `json:"dashboards"`
}

func (s *Service) encodeGetDashboardsResponse(w http.ResponseWriter, dashboards []*platform.Dashboard) {
	res := getDashboardsV2Response{
		Links: getDashboardsV2Links{
			Self: "/chronograf/v2/dashboards",
		},
		Dashboards: make([]dashboardV2Response, 0, len(dashboards)),
	}

	for _, dashboard := range dashboards {
		res.Dashboards = append(res.Dashboards, newDashboardV2Response(dashboard))
	}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// NewDashboardV2 creates a new dashboard.
func (s *Service) NewDashboardV2(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostDashboardRequest(ctx, r)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	if err := s.Store.DashboardsV2(ctx).CreateDashboard(ctx, req.Dashboard); err != nil {
		Error(w, http.StatusInternalServerError, fmt.Sprintf("Error loading dashboards: %v", err), s.Logger)
		return
	}

	s.encodePostDashboardResponse(w, req.Dashboard)
}

type postDashboardRequest struct {
	Dashboard *platform.Dashboard
}

func decodePostDashboardRequest(ctx context.Context, r *http.Request) (*postDashboardRequest, error) {
	c := &platform.Dashboard{}
	if err := json.NewDecoder(r.Body).Decode(c); err != nil {
		return nil, err
	}
	return &postDashboardRequest{
		Dashboard: c,
	}, nil
}

func (s *Service) encodePostDashboardResponse(w http.ResponseWriter, dashboard *platform.Dashboard) {
	encodeJSON(w, http.StatusCreated, newDashboardV2Response(dashboard), s.Logger)
}

// DashboardIDV2 retrieves a dashboard by ID.
func (s *Service) DashboardIDV2(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetDashboardRequest(ctx, r)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	dashboard, err := s.Store.DashboardsV2(ctx).FindDashboardByID(ctx, req.DashboardID)
	if err == platform.ErrDashboardNotFound {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
		return
	}

	if err != nil {
		Error(w, http.StatusInternalServerError, fmt.Sprintf("Error loading dashboard: %v", err), s.Logger)
		return
	}

	s.encodeGetDashboardResponse(w, dashboard)
}

type getDashboardRequest struct {
	DashboardID platform.ID
}

func decodeGetDashboardRequest(ctx context.Context, r *http.Request) (*getDashboardRequest, error) {
	param := httprouter.GetParamFromContext(ctx, "id")
	return &getDashboardRequest{
		DashboardID: platform.ID(param),
	}, nil
}

func (s *Service) encodeGetDashboardResponse(w http.ResponseWriter, dashboard *platform.Dashboard) {
	encodeJSON(w, http.StatusOK, newDashboardV2Response(dashboard), s.Logger)
}

// RemoveDashboardV2 removes a dashboard by ID.
func (s *Service) RemoveDashboardV2(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteDashboardRequest(ctx, r)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	err = s.Store.DashboardsV2(ctx).DeleteDashboard(ctx, req.DashboardID)
	if err == platform.ErrDashboardNotFound {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
		return
	}

	if err != nil {
		Error(w, http.StatusInternalServerError, fmt.Sprintf("Error deleting dashboard: %v", err), s.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type deleteDashboardRequest struct {
	DashboardID platform.ID
}

func decodeDeleteDashboardRequest(ctx context.Context, r *http.Request) (*deleteDashboardRequest, error) {
	param := httprouter.GetParamFromContext(ctx, "id")
	return &deleteDashboardRequest{
		DashboardID: platform.ID(param),
	}, nil
}

// UpdateDashboardV2 updates a dashboard.
func (s *Service) UpdateDashboardV2(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchDashboardRequest(ctx, r)
	if err != nil {
		Error(w, http.StatusBadRequest, err.Error(), s.Logger)
		return
	}
	dashboard, err := s.Store.DashboardsV2(ctx).UpdateDashboard(ctx, req.DashboardID, req.Upd)
	if err == platform.ErrDashboardNotFound {
		Error(w, http.StatusNotFound, err.Error(), s.Logger)
		return
	}

	if err != nil {
		Error(w, http.StatusInternalServerError, fmt.Sprintf("Error updating dashboard: %v", err), s.Logger)
		return
	}

	s.encodePatchDashboardResponse(w, dashboard)
}

type patchDashboardRequest struct {
	DashboardID platform.ID
	Upd         platform.DashboardUpdate
}

func decodePatchDashboardRequest(ctx context.Context, r *http.Request) (*patchDashboardRequest, error) {
	req := &patchDashboardRequest{}
	upd := platform.DashboardUpdate{}
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, err
	}
	req.Upd = upd

	param := httprouter.GetParamFromContext(ctx, "id")
	req.DashboardID = platform.ID(param)

	if err := req.Valid(); err != nil {
		return nil, err
	}

	return req, nil
}

// Valid validates that the dashboard ID is non zero valued and update has expected values set.
func (r *patchDashboardRequest) Valid() error {
	if r.DashboardID == "" {
		return fmt.Errorf("missing dashboard ID")
	}

	return r.Upd.Valid()
}

func (s *Service) encodePatchDashboardResponse(w http.ResponseWriter, dashboard *platform.Dashboard) {
	encodeJSON(w, http.StatusOK, newDashboardV2Response(dashboard), s.Logger)
}
