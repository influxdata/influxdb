package server

import (
	"fmt"
	"net/http"

	"github.com/influxdata/chronograf"
)

type dashboardLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

type dashboardResponse struct {
	chronograf.Dashboard
	Links dashboardLinks `json:"links"`
}

type getDashboardsResponse struct {
	Dashboards []dashboardResponse `json:"dashboards"`
}

func newDashboardResponse(d chronograf.Dashboard) dashboardResponse {
	base := "/chronograf/v1/dashboards"
	return dashboardResponse{
		Dashboard: d,
		Links: dashboardLinks{
			Self: fmt.Sprintf("%s/%d", base, d.ID),
		},
	}
}

// Dashboards returns all dashboards within the store
func (s *Service) Dashboards(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dashboards, err := s.DashboardsStore.All(ctx)
	if err != nil {
		Error(w, http.StatusInternalServerError, "Error loading layouts", s.Logger)
		return
	}

	res := getDashboardsResponse{
		Dashboards: []dashboardResponse{},
	}

	for _, dashboard := range dashboards {
		res.Dashboards = append(res.Dashboards, newDashboardResponse(dashboard))
	}

	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// DashboardID returns a single specified dashboard
func (s *Service) DashboardID(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	e, err := s.DashboardsStore.Get(ctx, chronograf.DashboardID(id))
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}

	res := newDashboardResponse(e)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// type postDashboardRequest struct {
// 	Data interface{} `json:"data"`           // Serialization of config.
// 	Name string      `json:"name,omitempty"` // Exploration name given by user.
// }

// NewDashboard creates and returns a new dashboard object
func (s *Service) NewDashboard(w http.ResponseWriter, r *http.Request) {

}

// RemoveDashboard deletes a dashboard
func (s *Service) RemoveDashboard(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	e, err := s.DashboardsStore.Get(ctx, chronograf.DashboardID(id))
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}

	if err := s.DashboardsStore.Delete(ctx, &chronograf.Dashboard{ID: chronograf.DashboardID(id)}); err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// UpdateDashboard updates a dashboard
func (s *Service) UpdateDashboard(w http.ResponseWriter, r *http.Request) {

}
