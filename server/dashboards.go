package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/chronograf"
)

type dashboardLinks struct {
	Self      string `json:"self"`      // Self link mapping to this resource
	Cells     string `json:"cells"`     // Cells link to the cells endpoint
	Templates string `json:"templates"` // Templates link to the templates endpoint
}

type dashboardResponse struct {
	ID        chronograf.DashboardID  `json:"id"`
	Cells     []dashboardCellResponse `json:"cells"`
	Templates []templateResponse      `json:"templates"`
	Name      string                  `json:"name"`
	Links     dashboardLinks          `json:"links"`
}

type getDashboardsResponse struct {
	Dashboards []*dashboardResponse `json:"dashboards"`
}

func newDashboardResponse(d chronograf.Dashboard) *dashboardResponse {
	base := "/chronograf/v1/dashboards"
	dd := AddQueryConfigs(DashboardDefaults(d))
	cells := newCellResponses(dd.ID, dd.Cells)
	templates := newTemplateResponses(dd.ID, dd.Templates)

	return &dashboardResponse{
		ID:        dd.ID,
		Name:      dd.Name,
		Cells:     cells,
		Templates: templates,
		Links: dashboardLinks{
			Self:      fmt.Sprintf("%s/%d", base, dd.ID),
			Cells:     fmt.Sprintf("%s/%d/cells", base, dd.ID),
			Templates: fmt.Sprintf("%s/%d/templates", base, dd.ID),
		},
	}
}

// Dashboards returns all dashboards within the store
func (s *Service) Dashboards(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dashboards, err := s.DashboardsStore.All(ctx)
	if err != nil {
		Error(w, http.StatusInternalServerError, "Error loading dashboards", s.Logger)
		return
	}

	res := getDashboardsResponse{
		Dashboards: []*dashboardResponse{},
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

// NewDashboard creates and returns a new dashboard object
func (s *Service) NewDashboard(w http.ResponseWriter, r *http.Request) {
	var dashboard chronograf.Dashboard
	if err := json.NewDecoder(r.Body).Decode(&dashboard); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err := ValidDashboardRequest(&dashboard); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	var err error
	if dashboard, err = s.DashboardsStore.Add(r.Context(), dashboard); err != nil {
		msg := fmt.Errorf("Error storing dashboard %v: %v", dashboard, err)
		unknownErrorWithMessage(w, msg, s.Logger)
		return
	}

	res := newDashboardResponse(dashboard)
	location(w, res.Links.Self)
	encodeJSON(w, http.StatusCreated, res, s.Logger)
}

// RemoveDashboard deletes a dashboard
func (s *Service) RemoveDashboard(w http.ResponseWriter, r *http.Request) {
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

	if err := s.DashboardsStore.Delete(ctx, e); err != nil {
		unknownErrorWithMessage(w, err, s.Logger)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ReplaceDashboard completely replaces a dashboard
func (s *Service) ReplaceDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	idParam, err := paramID("id", r)
	if err != nil {
		msg := fmt.Sprintf("Could not parse dashboard ID: %s", err)
		Error(w, http.StatusInternalServerError, msg, s.Logger)
	}
	id := chronograf.DashboardID(idParam)

	_, err = s.DashboardsStore.Get(ctx, id)
	if err != nil {
		Error(w, http.StatusNotFound, fmt.Sprintf("ID %d not found", id), s.Logger)
		return
	}

	var req chronograf.Dashboard
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}
	req.ID = id

	if err := ValidDashboardRequest(&req); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	if err := s.DashboardsStore.Update(ctx, req); err != nil {
		msg := fmt.Sprintf("Error updating dashboard ID %d: %v", id, err)
		Error(w, http.StatusInternalServerError, msg, s.Logger)
		return
	}

	res := newDashboardResponse(req)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// UpdateDashboard completely updates either the dashboard name or the cells
func (s *Service) UpdateDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	idParam, err := paramID("id", r)
	if err != nil {
		msg := fmt.Sprintf("Could not parse dashboard ID: %s", err)
		Error(w, http.StatusInternalServerError, msg, s.Logger)
		return
	}
	id := chronograf.DashboardID(idParam)

	orig, err := s.DashboardsStore.Get(ctx, id)
	if err != nil {
		Error(w, http.StatusNotFound, fmt.Sprintf("ID %d not found", id), s.Logger)
		return
	}

	var req chronograf.Dashboard
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, s.Logger)
		return
	}
	req.ID = id

	if req.Name != "" {
		orig.Name = req.Name
	} else if len(req.Cells) > 0 {
		if err := ValidDashboardRequest(&req); err != nil {
			invalidData(w, err, s.Logger)
			return
		}
		orig.Cells = req.Cells
	} else {
		invalidData(w, fmt.Errorf("Update must include either name or cells"), s.Logger)
		return
	}

	if err := s.DashboardsStore.Update(ctx, orig); err != nil {
		msg := fmt.Sprintf("Error updating dashboard ID %d: %v", id, err)
		Error(w, http.StatusInternalServerError, msg, s.Logger)
		return
	}

	res := newDashboardResponse(orig)
	encodeJSON(w, http.StatusOK, res, s.Logger)
}

// ValidDashboardRequest verifies that the dashboard cells have a query
func ValidDashboardRequest(d *chronograf.Dashboard) error {
	for i, c := range d.Cells {
		if err := ValidDashboardCellRequest(&c); err != nil {
			return err
		}
		d.Cells[i] = c
	}
	for _, t := range d.Templates {
		if err := ValidTemplateRequest(&t); err != nil {
			return err
		}
	}
	(*d) = DashboardDefaults(*d)
	return nil
}

// DashboardDefaults updates the dashboard with the default values
// if none are specified
func DashboardDefaults(d chronograf.Dashboard) (newDash chronograf.Dashboard) {
	newDash.ID = d.ID
	newDash.Templates = d.Templates
	newDash.Name = d.Name
	newDash.Cells = make([]chronograf.DashboardCell, len(d.Cells))

	for i, c := range d.Cells {
		CorrectWidthHeight(&c)
		newDash.Cells[i] = c
	}
	return
}

// AddQueryConfigs updates all the celsl in the dashboard to have query config
// objects corresponding to their influxql queries.
func AddQueryConfigs(d chronograf.Dashboard) (newDash chronograf.Dashboard) {
	newDash.ID = d.ID
	newDash.Templates = d.Templates
	newDash.Name = d.Name
	newDash.Cells = make([]chronograf.DashboardCell, len(d.Cells))

	for i, c := range d.Cells {
		AddQueryConfig(&c)
		newDash.Cells[i] = c
	}
	return
}
