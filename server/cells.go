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

type dashboardCellLinks struct {
	Self string `json:"self"` // Self link mapping to this resource
}

type dashboardCellResponse struct {
	chronograf.DashboardCell
	Links dashboardCellLinks `json:"links"`
}

func newCellResponses(dID chronograf.DashboardID, dcells []chronograf.DashboardCell) []dashboardCellResponse {
	base := "/chronograf/v1/dashboards"
	cells := make([]dashboardCellResponse, len(dcells))
	for i, cell := range dcells {
		newCell := chronograf.DashboardCell{}

		newCell.Queries = make([]chronograf.DashboardQuery, len(cell.Queries))
		copy(newCell.Queries, cell.Queries)

		// ensure x, y, and y2 axes always returned
		labels := []string{"x", "y", "y2"}
		newCell.Axes = make(map[string]chronograf.Axis, len(labels))

		newCell.X = cell.X
		newCell.Y = cell.Y
		newCell.W = cell.W
		newCell.H = cell.H
		newCell.Name = cell.Name
		newCell.ID = cell.ID
		newCell.Type = cell.Type

		for _, lbl := range labels {
			if axis, found := cell.Axes[lbl]; !found {
				newCell.Axes[lbl] = chronograf.Axis{
					Bounds: []string{},
				}
			} else {
				newCell.Axes[lbl] = axis
			}
		}

		cells[i] = dashboardCellResponse{
			DashboardCell: newCell,
			Links: dashboardCellLinks{
				Self: fmt.Sprintf("%s/%d/cells/%s", base, dID, cell.ID),
			},
		}
	}
	return cells
}

// ValidDashboardCellRequest verifies that the dashboard cells have a query and
// have the correct axes specified
func ValidDashboardCellRequest(c *chronograf.DashboardCell) error {
	CorrectWidthHeight(c)
	return HasCorrectAxes(c)
}

// HasCorrectAxes verifies that only permitted axes exist within a DashboardCell
func HasCorrectAxes(c *chronograf.DashboardCell) error {
	for axis, _ := range c.Axes {
		switch axis {
		case "x", "y", "y2":
			// no-op
		default:
			return chronograf.ErrInvalidAxis
		}
	}
	return nil
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

// DashboardCellID gets a specific cell from an existing dashboard
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

// RemoveDashboardCell removes a specific cell from an existing dashboard
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

// ReplaceDashboardCell replaces a cell entirely within an existing dashboard
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
