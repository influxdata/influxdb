package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	"github.com/influxdata/platform"
	kerrors "github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// DashboardHandler represents an HTTP API handler for dashboards.
type DashboardHandler struct {
	*httprouter.Router

	DashboardService platform.DashboardService
}

// NewDashboardHandler returns a new instance of DashboardHandler.
func NewDashboardHandler() *DashboardHandler {
	h := &DashboardHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", "/v1/dashboards", h.handlePostDashboard)
	h.HandlerFunc("GET", "/v1/dashboards", h.handleGetDashboards)
	h.HandlerFunc("GET", "/v1/dashboards/:id", h.handleGetDashboard)
	h.HandlerFunc("PATCH", "/v1/dashboards/:id", h.handlePatchDashboard)
	h.HandlerFunc("DELETE", "/v1/dashboards/:id", h.handleDeleteDashboard)

	h.HandlerFunc("POST", "/v1/dashboards/:id/cells", h.handlePostDashboardCell)
	h.HandlerFunc("PUT", "/v1/dashboards/:id/cells/:cell_id", h.handlePutDashboardCell)
	h.HandlerFunc("DELETE", "/v1/dashboards/:id/cells/:cell_id", h.handleDeleteDashboardCell)
	return h
}

// handlePostDashboard is the HTTP handler for the POST /v1/dashboards route.
func (h *DashboardHandler) handlePostDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostDashboardRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.DashboardService.CreateDashboard(ctx, req.Dashboard); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, req.Dashboard); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type postDashboardRequest struct {
	Dashboard *platform.Dashboard
}

func decodePostDashboardRequest(ctx context.Context, r *http.Request) (*postDashboardRequest, error) {
	b := &platform.Dashboard{}
	if err := json.NewDecoder(r.Body).Decode(b); err != nil {
		return nil, err
	}

	return &postDashboardRequest{
		Dashboard: b,
	}, nil
}

// handleGetDashboard is the HTTP handler for the GET /v1/dashboards/:id route.
func (h *DashboardHandler) handleGetDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetDashboardRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	b, err := h.DashboardService.FindDashboardByID(ctx, req.DashboardID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, b); err != nil {
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
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req := &getDashboardRequest{
		DashboardID: i,
	}

	return req, nil
}

// handleDeleteDashboard is the HTTP handler for the DELETE /v1/dashboards/:id route.
func (h *DashboardHandler) handleDeleteDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteDashboardRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.DashboardService.DeleteDashboard(ctx, req.DashboardID); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

type deleteDashboardRequest struct {
	DashboardID platform.ID
}

func decodeDeleteDashboardRequest(ctx context.Context, r *http.Request) (*deleteDashboardRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	req := &deleteDashboardRequest{
		DashboardID: i,
	}

	return req, nil
}

// handleGetDashboards is the HTTP handler for the GET /v1/dashboards route.
func (h *DashboardHandler) handleGetDashboards(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetDashboardsRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	bs, _, err := h.DashboardService.FindDashboards(ctx, req.filter)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, bs); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type getDashboardsRequest struct {
	filter platform.DashboardFilter
}

func decodeGetDashboardsRequest(ctx context.Context, r *http.Request) (*getDashboardsRequest, error) {
	qp := r.URL.Query()
	req := &getDashboardsRequest{}

	if id := qp.Get("orgID"); id != "" {
		req.filter.OrganizationID = &platform.ID{}
		if err := req.filter.OrganizationID.DecodeFromString(id); err != nil {
			return nil, err
		}
	}

	if org := qp.Get("org"); org != "" {
		req.filter.Organization = &org
	}

	if id := qp.Get("id"); id != "" {
		req.filter.ID = &platform.ID{}
		if err := req.filter.ID.DecodeFromString(id); err != nil {
			return nil, err
		}
	}

	return req, nil
}

// handlePatchDashboard is the HTTP handler for the PATH /v1/dashboards route.
func (h *DashboardHandler) handlePatchDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePatchDashboardRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	b, err := h.DashboardService.UpdateDashboard(ctx, req.DashboardID, req.Update)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, b); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type patchDashboardRequest struct {
	Update      platform.DashboardUpdate
	DashboardID platform.ID
}

func decodePatchDashboardRequest(ctx context.Context, r *http.Request) (*patchDashboardRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}

	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	var upd platform.DashboardUpdate
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, err
	}

	return &patchDashboardRequest{
		Update:      upd,
		DashboardID: i,
	}, nil
}

// handlePostDashboardCell is the HTTP handler for the POST /v1/dashboards/:id/cells route.
func (h *DashboardHandler) handlePostDashboardCell(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostDashboardCellRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.DashboardService.AddDashboardCell(ctx, req.DashboardID, req.DashboardCell); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, req.DashboardCell); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type postDashboardCellRequest struct {
	DashboardID   platform.ID
	DashboardCell *platform.DashboardCell
}

func decodePostDashboardCellRequest(ctx context.Context, r *http.Request) (*postDashboardCellRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}
	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	c := &platform.DashboardCell{}
	if err := json.NewDecoder(r.Body).Decode(c); err != nil {
		return nil, err
	}

	return &postDashboardCellRequest{
		DashboardCell: c,
		DashboardID:   i,
	}, nil
}

// handlePutDashboardCell is the HTTP handler for the PUT /v1/dashboards/:id/cells/:cell_id route.
func (h *DashboardHandler) handlePutDashboardCell(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePutDashboardCellRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.DashboardService.ReplaceDashboardCell(ctx, req.DashboardID, req.Cell); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, req.Cell); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type putDashboardCellRequest struct {
	DashboardID platform.ID
	Cell        *platform.DashboardCell
}

func decodePutDashboardCellRequest(ctx context.Context, r *http.Request) (*putDashboardCellRequest, error) {
	req := &putDashboardCellRequest{}
	params := httprouter.ParamsFromContext(ctx)

	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}
	if err := req.DashboardID.DecodeFromString(id); err != nil {
		return nil, err
	}

	cellID := params.ByName("cell_id")
	if cellID == "" {
		return nil, kerrors.InvalidDataf("url missing cell_id")
	}
	var cid platform.ID
	if err := cid.DecodeFromString(cellID); err != nil {
		return nil, err
	}

	req.Cell = &platform.DashboardCell{}
	if err := json.NewDecoder(r.Body).Decode(req.Cell); err != nil {
		return nil, err
	}

	if !bytes.Equal(req.Cell.ID, cid) {
		return nil, fmt.Errorf("url cell_id does not match id on provided cell")
	}

	return req, nil
}

// handleDeleteDashboardCell is the HTTP handler for the DELETE /v1/dashboards/:id/cells/:cell_id route.
func (h *DashboardHandler) handleDeleteDashboardCell(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteDashboardCellRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.DashboardService.RemoveDashboardCell(ctx, req.DashboardID, req.CellID); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

type deleteDashboardCellRequest struct {
	DashboardID platform.ID
	CellID      platform.ID
}

func decodeDeleteDashboardCellRequest(ctx context.Context, r *http.Request) (*deleteDashboardCellRequest, error) {
	req := &deleteDashboardCellRequest{}
	params := httprouter.ParamsFromContext(ctx)

	id := params.ByName("id")
	if id == "" {
		return nil, kerrors.InvalidDataf("url missing id")
	}
	if err := req.DashboardID.DecodeFromString(id); err != nil {
		return nil, err
	}

	cellID := params.ByName("cell_id")
	if cellID == "" {
		return nil, kerrors.InvalidDataf("url missing cell_id")
	}
	if err := req.CellID.DecodeFromString(cellID); err != nil {
		return nil, err
	}

	return req, nil
}

const (
	dashboardPath = "/v1/dashboards"
)

// DashboardService connects to Influx via HTTP using tokens to manage dashboards
type DashboardService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// FindDashboardByID returns a single dashboard by ID.
func (s *DashboardService) FindDashboardByID(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
	u, err := newURL(s.Addr, dashboardIDPath(id))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var b platform.Dashboard
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &b, nil
}

// FindDashboardsByOrganizationID returns a list of dashboards that match filter and the total count of matching dashboards.
func (s *DashboardService) FindDashboardsByOrganizationID(ctx context.Context, orgID platform.ID) ([]*platform.Dashboard, int, error) {
	return s.FindDashboards(ctx, platform.DashboardFilter{OrganizationID: &orgID})
}

// FindDashboardsByOrganizationName returns a list of dashboards that match filter and the total count of matching dashboards.
func (s *DashboardService) FindDashboardsByOrganizationName(ctx context.Context, org string) ([]*platform.Dashboard, int, error) {
	return s.FindDashboards(ctx, platform.DashboardFilter{Organization: &org})
}

// FindDashboards returns a list of dashboards that match filter and the total count of matching dashboards.
func (s *DashboardService) FindDashboards(ctx context.Context, filter platform.DashboardFilter) ([]*platform.Dashboard, int, error) {
	u, err := newURL(s.Addr, dashboardPath)
	if err != nil {
		return nil, 0, err
	}

	query := u.Query()
	if filter.OrganizationID != nil {
		query.Add("orgID", filter.OrganizationID.String())
	}
	if filter.Organization != nil {
		query.Add("org", *filter.Organization)
	}
	if filter.ID != nil {
		query.Add("id", filter.ID.String())
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	req.URL.RawQuery = query.Encode()
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var bs []*platform.Dashboard
	if err := json.NewDecoder(resp.Body).Decode(&bs); err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	return bs, len(bs), nil
}

// CreateDashboard creates a new dashboard and sets b.ID with the new identifier.
func (s *DashboardService) CreateDashboard(ctx context.Context, b *platform.Dashboard) error {
	u, err := newURL(s.Addr, dashboardPath)
	if err != nil {
		return err
	}

	octets, err := json.Marshal(b)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp); err != nil {
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(b); err != nil {
		return err
	}

	return nil
}

// UpdateDashboard updates a single dashboard with changeset.
// Returns the new dashboard state after update.
func (s *DashboardService) UpdateDashboard(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
	u, err := newURL(s.Addr, dashboardIDPath(id))
	if err != nil {
		return nil, err
	}

	octets, err := json.Marshal(upd)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PATCH", u.String(), bytes.NewReader(octets))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var b platform.Dashboard
	if err := json.NewDecoder(resp.Body).Decode(&b); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &b, nil
}

// DeleteDashboard removes a dashboard by ID.
func (s *DashboardService) DeleteDashboard(ctx context.Context, id platform.ID) error {
	u, err := newURL(s.Addr, dashboardIDPath(id))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	return CheckError(resp)
}

// AddDashboardCell adds a new cell to a dashboard.
func (s *DashboardService) AddDashboardCell(ctx context.Context, dashboardID platform.ID, cell *platform.DashboardCell) error {
	u, err := newURL(s.Addr, dashboardCellsPath(dashboardID))
	if err != nil {
		return err
	}

	octets, err := json.Marshal(cell)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	// TODO(jsternberg): Should this check for a 201 explicitly?
	if err := CheckError(resp); err != nil {
		return err
	}

	if err := json.NewDecoder(resp.Body).Decode(cell); err != nil {
		return err
	}

	return nil
}

// ReplaceDashboardCell replaces a single dashboard cell. It expects ID to be set on the provided cell.
func (s *DashboardService) ReplaceDashboardCell(ctx context.Context, dashboardID platform.ID, cell *platform.DashboardCell) error {
	u, err := newURL(s.Addr, dashboardCellPath(dashboardID, cell.ID))
	if err != nil {
		return err
	}
	octets, err := json.Marshal(cell)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", u.String(), bytes.NewReader(octets))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	return CheckError(resp)
}

// RemoveDashboardCell removes a cell from a dashboard.
func (s *DashboardService) RemoveDashboardCell(ctx context.Context, dashboardID platform.ID, cellID platform.ID) error {
	u, err := newURL(s.Addr, dashboardCellPath(dashboardID, cellID))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", s.Token)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	return CheckError(resp)
}

func dashboardIDPath(id platform.ID) string {
	return path.Join(dashboardPath, id.String())
}

func dashboardCellsPath(dashboardID platform.ID) string {
	// TODO: what to do about "cells" string
	return path.Join(dashboardIDPath(dashboardID), "cells")
}

func dashboardCellPath(dashboardID, cellID platform.ID) string {
	return path.Join(dashboardCellsPath(dashboardID), cellID.String())
}
