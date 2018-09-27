package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// DashboardHandler is the handler for the dashboard service
type DashboardHandler struct {
	*httprouter.Router

	DashboardService           platform.DashboardService
	UserResourceMappingService platform.UserResourceMappingService
}

const (
	dashboardsPath            = "/api/v2/dashboards"
	dashboardsIDPath          = "/api/v2/dashboards/:id"
	dashboardsIDCellsPath     = "/api/v2/dashboards/:id/cells"
	dashboardsIDCellsIDPath   = "/api/v2/dashboards/:id/cells/:cellID"
	dashboardsIDMembersPath   = "/api/v2/dashboards/:id/members"
	dashboardsIDMembersIDPath = "/api/v2/dashboards/:id/members/:userID"
	dashboardsIDOwnersPath    = "/api/v2/dashboards/:id/owners"
	dashboardsIDOwnersIDPath  = "/api/v2/dashboards/:id/owners/:userID"
)

// NewDashboardHandler returns a new instance of DashboardHandler.
func NewDashboardHandler() *DashboardHandler {
	h := &DashboardHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", dashboardsPath, h.handlePostDashboard)
	h.HandlerFunc("GET", dashboardsPath, h.handleGetDashboards)
	h.HandlerFunc("GET", dashboardsIDPath, h.handleGetDashboard)
	h.HandlerFunc("DELETE", dashboardsIDPath, h.handleDeleteDashboard)
	h.HandlerFunc("PATCH", dashboardsIDPath, h.handlePatchDashboard)

	h.HandlerFunc("PUT", dashboardsIDCellsPath, h.handlePutDashboardCells)
	h.HandlerFunc("POST", dashboardsIDCellsPath, h.handlePostDashboardCell)
	h.HandlerFunc("DELETE", dashboardsIDCellsIDPath, h.handleDeleteDashboardCell)
	h.HandlerFunc("PATCH", dashboardsIDCellsIDPath, h.handlePatchDashboardCell)

	h.HandlerFunc("POST", dashboardsIDMembersPath, newPostMemberHandler(h.UserResourceMappingService, platform.Member))

	h.HandlerFunc("POST", dashboardsIDOwnersPath, newPostMemberHandler(h.UserResourceMappingService, platform.Owner))
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

func (d dashboardResponse) toPlatform() *platform.Dashboard {
	if len(d.Cells) == 0 {
		return &platform.Dashboard{
			ID:   d.ID,
			Name: d.Name,
		}
	}

	cells := make([]*platform.Cell, 0, len(d.Cells))
	for i := range d.Cells {
		cells = append(cells, d.Cells[i].toPlatform())
	}
	return &platform.Dashboard{
		ID:    d.ID,
		Name:  d.Name,
		Cells: cells,
	}
}

func newDashboardResponse(d *platform.Dashboard) dashboardResponse {
	res := dashboardResponse{
		Links: dashboardLinks{
			Self:  fmt.Sprintf("/api/v2/dashboards/%s", d.ID),
			Cells: fmt.Sprintf("/api/v2/dashboards/%s/cells", d.ID),
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

func (c dashboardCellResponse) toPlatform() *platform.Cell {
	return &c.Cell
}

func newDashboardCellResponse(dashboardID platform.ID, c *platform.Cell) dashboardCellResponse {
	return dashboardCellResponse{
		Cell: *c,
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/dashboards/%s/cells/%s", dashboardID, c.ID),
			"view": fmt.Sprintf("/api/v2/views/%s", c.ViewID),
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
			"self": fmt.Sprintf("/api/v2/dashboards/%s/cells", dashboardID),
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
	req, err := decodeGetDashboardsRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	dashboards, _, err := h.DashboardService.FindDashboards(ctx, req.filter)
	if err != nil {
		EncodeError(ctx, errors.InternalErrorf("Error loading dashboards: %v", err), w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newGetDashboardsResponse(dashboards)); err != nil {
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

	if id := qp.Get("id"); id != "" {
		req.filter.ID = &platform.ID{}
		if err := req.filter.ID.DecodeFromString(id); err != nil {
			return nil, err
		}
	}
	return req, nil
}

type getDashboardsLinks struct {
	Self string `json:"self"`
}

type getDashboardsResponse struct {
	Links      getDashboardsLinks  `json:"links"`
	Dashboards []dashboardResponse `json:"dashboards"`
}

func (d getDashboardsResponse) toPlatform() []*platform.Dashboard {
	res := make([]*platform.Dashboard, len(d.Dashboards))
	for i := range d.Dashboards {
		res[i] = d.Dashboards[i].toPlatform()
	}
	return res
}

func newGetDashboardsResponse(dashboards []*platform.Dashboard) getDashboardsResponse {
	res := getDashboardsResponse{
		Links: getDashboardsLinks{
			Self: "/api/v2/dashboards",
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

// DashboardService is a dashboard service over HTTP to the influxdb server.
type DashboardService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// FindDashboardByID returns a single dashboard by ID.
func (s *DashboardService) FindDashboardByID(ctx context.Context, id platform.ID) (*platform.Dashboard, error) {
	path := dashboardIDPath(id)
	url, err := newURL(s.Addr, path)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}

	SetToken(s.Token, req)
	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var dr dashboardResponse
	if err := json.NewDecoder(resp.Body).Decode(&dr); err != nil {
		return nil, err
	}

	dashboard := dr.toPlatform()
	return dashboard, nil
}

// FindDashboards returns a list of dashboards that match filter and the total count of matching dashboards.
// Additional options provide pagination & sorting.
func (s *DashboardService) FindDashboards(ctx context.Context, filter platform.DashboardFilter) ([]*platform.Dashboard, int, error) {
	url, err := newURL(s.Addr, dashboardsPath)
	if err != nil {
		return nil, 0, err
	}

	qp := url.Query()
	if filter.ID != nil {
		qp.Add("id", filter.ID.String())
	}
	url.RawQuery = qp.Encode()

	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, 0, err
	}

	SetToken(s.Token, req)
	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if err := CheckError(resp); err != nil {
		return nil, 0, err
	}

	var dr getDashboardsResponse
	if err := json.NewDecoder(resp.Body).Decode(&dr); err != nil {
		return nil, 0, err
	}

	dashboards := dr.toPlatform()
	return dashboards, len(dashboards), nil
}

// CreateDashboard creates a new dashboard and sets b.ID with the new identifier.
func (s *DashboardService) CreateDashboard(ctx context.Context, d *platform.Dashboard) error {
	url, err := newURL(s.Addr, dashboardsPath)
	if err != nil {
		return err
	}

	b, err := json.Marshal(d)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url.String(), bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	if err := CheckError(resp); err != nil {
		return err
	}

	return json.NewDecoder(resp.Body).Decode(d)
}

// UpdateDashboard updates a single dashboard with changeset.
// Returns the new dashboard state after update.
func (s *DashboardService) UpdateDashboard(ctx context.Context, id platform.ID, upd platform.DashboardUpdate) (*platform.Dashboard, error) {
	u, err := newURL(s.Addr, dashboardIDPath(id))
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(upd)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PATCH", u.String(), bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var d platform.Dashboard
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if len(d.Cells) == 0 {
		d.Cells = nil
	}

	return &d, nil
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
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	return CheckError(resp)
}

// AddDashboardCell adds a cell to a dashboard.
func (s *DashboardService) AddDashboardCell(ctx context.Context, id platform.ID, c *platform.Cell, opts platform.AddDashboardCellOptions) error {
	url, err := newURL(s.Addr, cellPath(id))
	if err != nil {
		return err
	}

	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url.String(), bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(url.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	if err := CheckError(resp); err != nil {
		return err
	}

	// TODO (goller): deal with the dashboard cell options
	return json.NewDecoder(resp.Body).Decode(c)
}

// RemoveDashboardCell removes a dashboard.
func (s *DashboardService) RemoveDashboardCell(ctx context.Context, dashboardID, cellID platform.ID) error {
	u, err := newURL(s.Addr, dashboardCellIDPath(dashboardID, cellID))
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	return CheckError(resp)
}

// UpdateDashboardCell replaces the dashboard cell with the provided ID.
func (s *DashboardService) UpdateDashboardCell(ctx context.Context, dashboardID, cellID platform.ID, upd platform.CellUpdate) (*platform.Cell, error) {
	u, err := newURL(s.Addr, dashboardCellIDPath(dashboardID, cellID))
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(upd)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PATCH", u.String(), bytes.NewReader(b))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(resp); err != nil {
		return nil, err
	}

	var c platform.Cell
	if err := json.NewDecoder(resp.Body).Decode(&c); err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return &c, nil
}

// ReplaceDashboardCells replaces all cells in a dashboard
func (s *DashboardService) ReplaceDashboardCells(ctx context.Context, id platform.ID, cs []*platform.Cell) error {
	u, err := newURL(s.Addr, cellPath(id))
	if err != nil {
		return err
	}

	// TODO(goller): I think this should be {"cells":[]}
	b, err := json.Marshal(cs)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("PUT", u.String(), bytes.NewReader(b))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	SetToken(s.Token, req)

	hc := newClient(u.Scheme, s.InsecureSkipVerify)

	resp, err := hc.Do(req)
	if err != nil {
		return err
	}

	if err := CheckError(resp); err != nil {
		return err
	}

	cells := dashboardCellsResponse{}
	if err := json.NewDecoder(resp.Body).Decode(&cells); err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func dashboardIDPath(id platform.ID) string {
	return path.Join(dashboardsPath, id.String())
}

func cellPath(id platform.ID) string {
	return path.Join(dashboardIDPath(id), "cells")
}

func dashboardCellIDPath(id platform.ID, cellID platform.ID) string {
	return path.Join(cellPath(id), cellID.String())
}
