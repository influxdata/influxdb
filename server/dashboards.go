package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
	"github.com/influxdata/chronograf/uuid"
)

const (
	// DefaultWidth is used if not specified
	DefaultWidth = 4
	// DefaultHeight is used if not specified
	DefaultHeight = 4
)

type dashboardLinks struct {
	Self  string `json:"self"`  // Self link mapping to this resource
	Cells string `json:"cells"` // Cells link to the cells endpoint
}

type dashboardCellLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

type dashboardCellResponse struct {
	chronograf.DashboardCell
	Links dashboardCellLinks `json:"links"`
}

type dashboardResponse struct {
	ID    chronograf.DashboardID  `json:"id"`
	Cells []dashboardCellResponse `json:"cells"`
	Name  string                  `json:"name"`
	Links dashboardLinks          `json:"links"`
}

type getDashboardsResponse struct {
	Dashboards []*dashboardResponse `json:"dashboards"`
}

func newDashboardResponse(d chronograf.Dashboard) *dashboardResponse {
	base := "/chronograf/v1/dashboards"
	DashboardDefaults(&d)
	AddQueryConfigs(&d)
	cells := make([]dashboardCellResponse, len(d.Cells))
	for i, cell := range d.Cells {
		if len(cell.Queries) == 0 {
			cell.Queries = make([]chronograf.DashboardQuery, 0)
		}
		cells[i] = dashboardCellResponse{
			DashboardCell: cell,
			Links: dashboardCellLinks{
				Self: fmt.Sprintf("%s/%d/cells/%s", base, d.ID, cell.ID),
			},
		}
	}
	return &dashboardResponse{
		ID:    d.ID,
		Name:  d.Name,
		Cells: cells,
		Links: dashboardLinks{
			Self:  fmt.Sprintf("%s/%d", base, d.ID),
			Cells: fmt.Sprintf("%s/%d/cells", base, d.ID),
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
	w.Header().Add("Location", res.Links.Self)
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
		CorrectWidthHeight(&c)
		d.Cells[i] = c
	}
	DashboardDefaults(d)
	return nil
}

// ValidDashboardCellRequest verifies that the dashboard cells have a query
func ValidDashboardCellRequest(c *chronograf.DashboardCell) error {
	CorrectWidthHeight(c)
	return nil
}

// DashboardDefaults updates the dashboard with the default values
// if none are specified
func DashboardDefaults(d *chronograf.Dashboard) {
	for i, c := range d.Cells {
		CorrectWidthHeight(&c)
		d.Cells[i] = c
	}
}

// CorrectWidthHeight changes the cell to have at least the
// minimum width and height
func CorrectWidthHeight(c *chronograf.DashboardCell) {
	if c.W < 1 {
		c.W = DefaultWidth
	}
	if c.H < 1 {
		c.H = DefaultHeight
	}
}

// AddQueryConfigs updates all the celsl in the dashboard to have query config
// objects corresponding to their influxql queries.
func AddQueryConfigs(d *chronograf.Dashboard) {
	for i, c := range d.Cells {
		AddQueryConfig(&c)
		d.Cells[i] = c
	}
}

// AddQueryConfig updates a cell by converting InfluxQL into queryconfigs
// If influxql cannot be represented by a full query config, then, the
// query config's raw text is set to the command.
func AddQueryConfig(c *chronograf.DashboardCell) {
	for i, q := range c.Queries {
		qc := ToQueryConfig(q.Command)
		q.QueryConfig = qc
		c.Queries[i] = q
	}
}

// DashboardCells returns all cells from a dashboard within the store
func (s *Service) DashboardCells(w http.ResponseWriter, r *http.Request) {
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

	boards := newDashboardResponse(e)
	cells := boards.Cells
	encodeJSON(w, http.StatusOK, cells, s.Logger)
}

// NewDashboardCell adds a cell to an existing dashboard
func (s *Service) NewDashboardCell(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	dash, err := s.DashboardsStore.Get(ctx, chronograf.DashboardID(id))
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}
	var cell chronograf.DashboardCell
	if err := json.NewDecoder(r.Body).Decode(&cell); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err := ValidDashboardCellRequest(&cell); err != nil {
		invalidData(w, err, s.Logger)
		return
	}

	ids := uuid.V4{}
	cid, err := ids.Generate()
	if err != nil {
		msg := fmt.Sprintf("Error creating cell ID of dashboard %d: %v", id, err)
		Error(w, http.StatusInternalServerError, msg, s.Logger)
		return
	}
	cell.ID = cid

	dash.Cells = append(dash.Cells, cell)
	if err := s.DashboardsStore.Update(ctx, dash); err != nil {
		msg := fmt.Sprintf("Error adding cell %s to dashboard %d: %v", cid, id, err)
		Error(w, http.StatusInternalServerError, msg, s.Logger)
		return
	}

	boards := newDashboardResponse(dash)
	for _, cell := range boards.Cells {
		if cell.ID == cid {
			encodeJSON(w, http.StatusOK, cell, s.Logger)
			return
		}
	}
}

// DashboardCellID adds a cell to an existing dashboard
func (s *Service) DashboardCellID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	dash, err := s.DashboardsStore.Get(ctx, chronograf.DashboardID(id))
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}

	boards := newDashboardResponse(dash)
	cid := httprouter.GetParamFromContext(ctx, "cid")
	for _, cell := range boards.Cells {
		if cell.ID == cid {
			encodeJSON(w, http.StatusOK, cell, s.Logger)
			return
		}
	}
	notFound(w, id, s.Logger)
}

// RemoveDashboardCell adds a cell to an existing dashboard
func (s *Service) RemoveDashboardCell(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	dash, err := s.DashboardsStore.Get(ctx, chronograf.DashboardID(id))
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}

	cid := httprouter.GetParamFromContext(ctx, "cid")
	cellid := -1
	for i, cell := range dash.Cells {
		if cell.ID == cid {
			cellid = i
			break
		}
	}
	if cellid == -1 {
		notFound(w, id, s.Logger)
		return
	}

	dash.Cells = append(dash.Cells[:cellid], dash.Cells[cellid+1:]...)
	if err := s.DashboardsStore.Update(ctx, dash); err != nil {
		msg := fmt.Sprintf("Error removing cell %s from dashboard %d: %v", cid, id, err)
		Error(w, http.StatusInternalServerError, msg, s.Logger)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ReplaceDashboardCell adds a cell to an existing dashboard
func (s *Service) ReplaceDashboardCell(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), s.Logger)
		return
	}

	ctx := r.Context()
	dash, err := s.DashboardsStore.Get(ctx, chronograf.DashboardID(id))
	if err != nil {
		notFound(w, id, s.Logger)
		return
	}

	cid := httprouter.GetParamFromContext(ctx, "cid")
	cellid := -1
	for i, cell := range dash.Cells {
		if cell.ID == cid {
			cellid = i
			break
		}
	}
	if cellid == -1 {
		notFound(w, id, s.Logger)
		return
	}

	var cell chronograf.DashboardCell
	if err := json.NewDecoder(r.Body).Decode(&cell); err != nil {
		invalidJSON(w, s.Logger)
		return
	}

	if err := ValidDashboardCellRequest(&cell); err != nil {
		invalidData(w, err, s.Logger)
		return
	}
	cell.ID = cid

	dash.Cells[cellid] = cell
	if err := s.DashboardsStore.Update(ctx, dash); err != nil {
		msg := fmt.Sprintf("Error updating cell %s in dashboard %d: %v", cid, id, err)
		Error(w, http.StatusInternalServerError, msg, s.Logger)
		return
	}

	boards := newDashboardResponse(dash)
	for _, cell := range boards.Cells {
		if cell.ID == cid {
			encodeJSON(w, http.StatusOK, cell, s.Logger)
			return
		}
	}
}
