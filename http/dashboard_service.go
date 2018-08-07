package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// DashboardHandler is the handler for the dashboard service
type DashboardHandler struct {
	*httprouter.Router

	DashboardService platform.DashboardService
}

// NewDashboardHandler returns a new instance of DashboardHandler.
func NewDashboardHandler() *DashboardHandler {
	h := &DashboardHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", "/v2/dashboards", h.handlePostDashboard)
	h.HandlerFunc("GET", "/v2/dashboards", h.handleGetDashboards)
	h.HandlerFunc("GET", "/v2/dashboards/:id", h.handleGetDashboard)
	h.HandlerFunc("DELETE", "/v2/dashboards/:id", h.handleDeleteDashboard)
	h.HandlerFunc("PATCH", "/v2/dashboards/:id", h.handlePatchDashboard)

	h.HandlerFunc("PUT", "/v2/dashboards/:id/cells", h.handlePutDashboardCells)
	h.HandlerFunc("POST", "/v2/dashboards/:id/cells", h.handlePostDashboardCell)
	h.HandlerFunc("DELETE", "/v2/dashboards/:id/cells/:cellID", h.handleDeleteDashboardCell)
	h.HandlerFunc("PATCH", "/v2/dashboards/:id/cells/:cellID", h.handlePatchDashboardCell)
	return h
}

type dashboardLinks struct {
	Self  string `json:"self"`
	Cells string `json:"cells"`
}

type dashboardResponse struct {
	platform.Dashboard
	Cells []dashboardCellResponse `json:"cells"`
	Links dashboardLinks          `json:"links"`
}

func newDashboardResponse(d *platform.Dashboard) dashboardResponse {
	res := dashboardResponse{
		Links: dashboardLinks{
			Self:  fmt.Sprintf("/v2/dashboards/%s", d.ID),
			Cells: fmt.Sprintf("/v2/dashboards/%s/cells", d.ID),
		},
		Dashboard: *d,
		Cells:     []dashboardCellResponse{},
	}

	for _, cell := range d.Cells {
		res.Cells = append(res.Cells, newDashboardCellResponse(d.ID, cell))
	}

	return res
}

type dashboardCellResponse struct {
	platform.Cell
	Links map[string]string `json:"links"`
}

func newDashboardCellResponse(dashboardID platform.ID, c *platform.Cell) dashboardCellResponse {
	return dashboardCellResponse{
		Cell: *c,
		Links: map[string]string{
			"self": fmt.Sprintf("/v2/dashboards/%s/cells/%s", dashboardID, c.ID),
			"view": fmt.Sprintf("/v2/views/%s", c.ViewID),
		},
	}
}

type dashboardCellsResponse struct {
	Cells []dashboardCellResponse `json:"cells"`
	Links map[string]string       `json:"links"`
}

func newDashboardCellsResponse(dashboardID platform.ID, cs []*platform.Cell) dashboardCellsResponse {
	res := dashboardCellsResponse{
		Cells: []dashboardCellResponse{},
		Links: map[string]string{
			"self": fmt.Sprintf("/v2/dashboards/%s/cells", dashboardID),
		},
	}

	for _, cell := range cs {
		res.Cells = append(res.Cells, newDashboardCellResponse(dashboardID, cell))
	}

	return res
}

// handleGetDashboards returns all dashboards within the store.
func (h *DashboardHandler) handleGetDashboards(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// TODO(desa): support filtering via query params
	dashboards, _, err := h.DashboardService.FindDashboards(ctx, platform.DashboardFilter{})
	if err != nil {
		EncodeError(ctx, errors.InternalErrorf("Error loading dashboards: %v", err), w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newGetDashboardsResponse(dashboards)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getDashboardsLinks struct {
	Self string `json:"self"`
}

type getDashboardsResponse struct {
	Links      getDashboardsLinks  `json:"links"`
	Dashboards []dashboardResponse `json:"dashboards"`
}

func newGetDashboardsResponse(dashboards []*platform.Dashboard) getDashboardsResponse {
	res := getDashboardsResponse{
		Links: getDashboardsLinks{
			Self: "/v2/dashboards",
		},
		Dashboards: make([]dashboardResponse, 0, len(dashboards)),
	}

	for _, dashboard := range dashboards {
		res.Dashboards = append(res.Dashboards, newDashboardResponse(dashboard))
	}

	return res
}

// handlePostDashboard creates a new dashboard.
func (h *DashboardHandler) handlePostDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostDashboardRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	if err := h.DashboardService.CreateDashboard(ctx, req.Dashboard); err != nil {
		EncodeError(ctx, errors.InternalErrorf("Error loading dashboards: %v", err), w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newDashboardResponse(req.Dashboard)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
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

// hanldeGetDashboard retrieves a dashboard by ID.
func (h *DashboardHandler) handleGetDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetDashboardRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	dashboard, err := h.DashboardService.FindDashboardByID(ctx, req.DashboardID)
	if err != nil {
		if err == platform.ErrDashboardNotFound {
			err = errors.New(err.Error(), errors.NotFound)
		}
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newDashboardResponse(dashboard)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getDashboardRequest struct {
	DashboardID platform.ID
}

func decodeGetDashboardRequest(ctx context.Context, r *http.Request) (*getDashboardRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &getDashboardRequest{
		DashboardID: i,
	}, nil
}

// handleDeleteDashboard removes a dashboard by ID.
func (h *DashboardHandler) handleDeleteDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteDashboardRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.DashboardService.DeleteDashboard(ctx, req.DashboardID); err != nil {
		if err == platform.ErrDashboardNotFound {
			err = errors.New(err.Error(), errors.NotFound)
		}
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type deleteDashboardRequest struct {
	DashboardID platform.ID
}

func decodeDeleteDashboardRequest(ctx context.Context, r *http.Request) (*deleteDashboardRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	return &deleteDashboardRequest{
		DashboardID: i,
	}, nil
}

// handlePatchDashboard updates a dashboard.
func (h *DashboardHandler) handlePatchDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchDashboardRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	dashboard, err := h.DashboardService.UpdateDashboard(ctx, req.DashboardID, req.Upd)
	if err != nil {
		if err == platform.ErrDashboardNotFound {
			err = errors.New(err.Error(), errors.NotFound)
		}
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newDashboardResponse(dashboard)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type patchDashboardRequest struct {
	DashboardID platform.ID
	Upd         platform.DashboardUpdate
}

func decodePatchDashboardRequest(ctx context.Context, r *http.Request) (*patchDashboardRequest, error) {
	req := &patchDashboardRequest{}
	upd := platform.DashboardUpdate{}
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, errors.MalformedDataf(err.Error())
	}

	req.Upd = upd

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}
	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	req.DashboardID = i

	if err := req.Valid(); err != nil {
		return nil, errors.MalformedDataf(err.Error())
	}

	return req, nil
}

// Valid validates that the dashboard ID is non zero valued and update has expected values set.
func (r *patchDashboardRequest) Valid() error {
	if len(r.DashboardID) == 0 {
		return fmt.Errorf("missing dashboard ID")
	}

	return r.Upd.Valid()
}

type postDashboardCellRequest struct {
	dashboardID platform.ID
	cell        *platform.Cell
	opts        platform.AddDashboardCellOptions
}

func decodePostDashboardCellRequest(ctx context.Context, r *http.Request) (*postDashboardCellRequest, error) {
	req := &postDashboardCellRequest{}

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}
	if err := req.dashboardID.DecodeFromString(id); err != nil {
		return nil, err
	}

	bs, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	req.cell = &platform.Cell{}
	if err := json.NewDecoder(bytes.NewReader(bs)).Decode(req.cell); err != nil {
		return nil, err
	}
	if err := json.NewDecoder(bytes.NewReader(bs)).Decode(&req.opts); err != nil {
		return nil, err
	}

	return req, nil
}

// handlePostDashboardCell creates a dashboard cell.
func (h *DashboardHandler) handlePostDashboardCell(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostDashboardCellRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	if err := h.DashboardService.AddDashboardCell(ctx, req.dashboardID, req.cell, req.opts); err != nil {
		if err == platform.ErrDashboardNotFound {
			err = errors.New(err.Error(), errors.NotFound)
		}
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newDashboardCellResponse(req.dashboardID, req.cell)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type putDashboardCellRequest struct {
	dashboardID platform.ID
	cells       []*platform.Cell
}

func decodePutDashboardCellRequest(ctx context.Context, r *http.Request) (*putDashboardCellRequest, error) {
	req := &putDashboardCellRequest{}

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}
	if err := req.dashboardID.DecodeFromString(id); err != nil {
		return nil, err
	}

	req.cells = []*platform.Cell{}
	if err := json.NewDecoder(r.Body).Decode(&req.cells); err != nil {
		return nil, err
	}

	return req, nil
}

// handlePutDashboardCells replaces a dashboards cells.
func (h *DashboardHandler) handlePutDashboardCells(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePutDashboardCellRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.DashboardService.ReplaceDashboardCells(ctx, req.dashboardID, req.cells); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newDashboardCellsResponse(req.dashboardID, req.cells)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type deleteDashboardCellRequest struct {
	dashboardID platform.ID
	cellID      platform.ID
}

func decodeDeleteDashboardCellRequest(ctx context.Context, r *http.Request) (*deleteDashboardCellRequest, error) {
	req := &deleteDashboardCellRequest{}

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}
	if err := req.dashboardID.DecodeFromString(id); err != nil {
		return nil, err
	}

	cellID := params.ByName("cellID")
	if cellID == "" {
		return nil, errors.InvalidDataf("url missing cellID")
	}
	if err := req.cellID.DecodeFromString(cellID); err != nil {
		return nil, err
	}

	return req, nil
}

// handleDeleteDashboardCell deletes a dashboard cell.
func (h *DashboardHandler) handleDeleteDashboardCell(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteDashboardCellRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	if err := h.DashboardService.RemoveDashboardCell(ctx, req.dashboardID, req.cellID); err != nil {
		if err == platform.ErrDashboardNotFound || err == platform.ErrCellNotFound {
			err = errors.New(err.Error(), errors.NotFound)
		}
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type patchDashboardCellRequest struct {
	dashboardID platform.ID
	cellID      platform.ID
	upd         platform.CellUpdate
}

func decodePatchDashboardCellRequest(ctx context.Context, r *http.Request) (*patchDashboardCellRequest, error) {
	req := &patchDashboardCellRequest{}

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, errors.InvalidDataf("url missing id")
	}
	if err := req.dashboardID.DecodeFromString(id); err != nil {
		return nil, err
	}

	cellID := params.ByName("cellID")
	if cellID == "" {
		return nil, errors.InvalidDataf("url missing cellID")
	}
	if err := req.cellID.DecodeFromString(cellID); err != nil {
		return nil, err
	}

	if err := json.NewDecoder(r.Body).Decode(&req.upd); err != nil {
		return nil, errors.MalformedDataf(err.Error())
	}

	return req, nil
}

// handlePatchDashboardCell updates a dashboard cell.
func (h *DashboardHandler) handlePatchDashboardCell(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchDashboardCellRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	cell, err := h.DashboardService.UpdateDashboardCell(ctx, req.dashboardID, req.cellID, req.upd)
	if err != nil {
		if err == platform.ErrDashboardNotFound || err == platform.ErrCellNotFound {
			err = errors.New(err.Error(), errors.NotFound)
		}
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newDashboardCellResponse(req.dashboardID, cell)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}
