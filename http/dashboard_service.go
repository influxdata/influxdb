package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"go.uber.org/zap"
)

// DashboardBackend is all services and associated parameters required to construct
// the DashboardHandler.
type DashboardBackend struct {
	influxdb.HTTPErrorHandler
	log *zap.Logger

	DashboardService             influxdb.DashboardService
	DashboardOperationLogService influxdb.DashboardOperationLogService
	UserResourceMappingService   influxdb.UserResourceMappingService
	LabelService                 influxdb.LabelService
	UserService                  influxdb.UserService
}

// NewDashboardBackend creates a backend used by the dashboard handler.
func NewDashboardBackend(log *zap.Logger, b *APIBackend) *DashboardBackend {
	return &DashboardBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		DashboardService:             b.DashboardService,
		DashboardOperationLogService: b.DashboardOperationLogService,
		UserResourceMappingService:   b.UserResourceMappingService,
		LabelService:                 b.LabelService,
		UserService:                  b.UserService,
	}
}

// DashboardHandler is the handler for the dashboard service
type DashboardHandler struct {
	*httprouter.Router

	influxdb.HTTPErrorHandler
	log *zap.Logger

	DashboardService             influxdb.DashboardService
	DashboardOperationLogService influxdb.DashboardOperationLogService
	UserResourceMappingService   influxdb.UserResourceMappingService
	LabelService                 influxdb.LabelService
	UserService                  influxdb.UserService
}

const (
	prefixDashboards            = "/api/v2/dashboards"
	dashboardsIDPath            = "/api/v2/dashboards/:id"
	dashboardsIDCellsPath       = "/api/v2/dashboards/:id/cells"
	dashboardsIDCellsIDPath     = "/api/v2/dashboards/:id/cells/:cellID"
	dashboardsIDCellsIDViewPath = "/api/v2/dashboards/:id/cells/:cellID/view"
	dashboardsIDMembersPath     = "/api/v2/dashboards/:id/members"
	dashboardsIDMembersIDPath   = "/api/v2/dashboards/:id/members/:userID"
	dashboardsIDOwnersPath      = "/api/v2/dashboards/:id/owners"
	dashboardsIDOwnersIDPath    = "/api/v2/dashboards/:id/owners/:userID"
	dashboardsIDLabelsPath      = "/api/v2/dashboards/:id/labels"
	dashboardsIDLabelsIDPath    = "/api/v2/dashboards/:id/labels/:lid"
)

// NewDashboardHandler returns a new instance of DashboardHandler.
func NewDashboardHandler(log *zap.Logger, b *DashboardBackend) *DashboardHandler {
	h := &DashboardHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		DashboardService:             b.DashboardService,
		DashboardOperationLogService: b.DashboardOperationLogService,
		UserResourceMappingService:   b.UserResourceMappingService,
		LabelService:                 b.LabelService,
		UserService:                  b.UserService,
	}

	h.HandlerFunc("POST", prefixDashboards, h.handlePostDashboard)
	h.HandlerFunc("GET", prefixDashboards, h.handleGetDashboards)
	h.HandlerFunc("GET", dashboardsIDPath, h.handleGetDashboard)
	h.HandlerFunc("DELETE", dashboardsIDPath, h.handleDeleteDashboard)
	h.HandlerFunc("PATCH", dashboardsIDPath, h.handlePatchDashboard)

	h.HandlerFunc("PUT", dashboardsIDCellsPath, h.handlePutDashboardCells)
	h.HandlerFunc("POST", dashboardsIDCellsPath, h.handlePostDashboardCell)
	h.HandlerFunc("DELETE", dashboardsIDCellsIDPath, h.handleDeleteDashboardCell)
	h.HandlerFunc("PATCH", dashboardsIDCellsIDPath, h.handlePatchDashboardCell)

	h.HandlerFunc("GET", dashboardsIDCellsIDViewPath, h.handleGetDashboardCellView)
	h.HandlerFunc("PATCH", dashboardsIDCellsIDViewPath, h.handlePatchDashboardCellView)

	memberBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               influxdb.DashboardsResourceType,
		UserType:                   influxdb.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", dashboardsIDMembersPath, newPostMemberHandler(memberBackend))
	h.HandlerFunc("GET", dashboardsIDMembersPath, newGetMembersHandler(memberBackend))
	h.HandlerFunc("DELETE", dashboardsIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               influxdb.DashboardsResourceType,
		UserType:                   influxdb.Owner,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", dashboardsIDOwnersPath, newPostMemberHandler(ownerBackend))
	h.HandlerFunc("GET", dashboardsIDOwnersPath, newGetMembersHandler(ownerBackend))
	h.HandlerFunc("DELETE", dashboardsIDOwnersIDPath, newDeleteMemberHandler(ownerBackend))

	labelBackend := &LabelBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              b.log.With(zap.String("handler", "label")),
		LabelService:     b.LabelService,
		ResourceType:     influxdb.DashboardsResourceType,
	}
	h.HandlerFunc("GET", dashboardsIDLabelsPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", dashboardsIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", dashboardsIDLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

type dashboardLinks struct {
	Self         string `json:"self"`
	Members      string `json:"members"`
	Owners       string `json:"owners"`
	Cells        string `json:"cells"`
	Labels       string `json:"labels"`
	Organization string `json:"org"`
}

type dashboardResponse struct {
	ID             influxdb.ID             `json:"id,omitempty"`
	OrganizationID influxdb.ID             `json:"orgID,omitempty"`
	Name           string                  `json:"name"`
	Description    string                  `json:"description"`
	Meta           influxdb.DashboardMeta  `json:"meta"`
	Cells          []dashboardCellResponse `json:"cells"`
	Labels         []influxdb.Label        `json:"labels"`
	Links          dashboardLinks          `json:"links"`
}

func (d dashboardResponse) toinfluxdb() *influxdb.Dashboard {
	var cells []*influxdb.Cell
	if len(d.Cells) > 0 {
		cells = make([]*influxdb.Cell, len(d.Cells))
	}
	for i := range d.Cells {
		cells[i] = d.Cells[i].toinfluxdb()
	}
	return &influxdb.Dashboard{
		ID:             d.ID,
		OrganizationID: d.OrganizationID,
		Name:           d.Name,
		Description:    d.Description,
		Meta:           d.Meta,
		Cells:          cells,
	}
}

func newDashboardResponse(d *influxdb.Dashboard, labels []*influxdb.Label) dashboardResponse {
	res := dashboardResponse{
		Links: dashboardLinks{
			Self:         fmt.Sprintf("/api/v2/dashboards/%s", d.ID),
			Members:      fmt.Sprintf("/api/v2/dashboards/%s/members", d.ID),
			Owners:       fmt.Sprintf("/api/v2/dashboards/%s/owners", d.ID),
			Cells:        fmt.Sprintf("/api/v2/dashboards/%s/cells", d.ID),
			Labels:       fmt.Sprintf("/api/v2/dashboards/%s/labels", d.ID),
			Organization: fmt.Sprintf("/api/v2/orgs/%s", d.OrganizationID),
		},
		ID:             d.ID,
		OrganizationID: d.OrganizationID,
		Name:           d.Name,
		Description:    d.Description,
		Meta:           d.Meta,
		Labels:         []influxdb.Label{},
		Cells:          []dashboardCellResponse{},
	}

	for _, l := range labels {
		res.Labels = append(res.Labels, *l)
	}

	for _, cell := range d.Cells {
		res.Cells = append(res.Cells, newDashboardCellResponse(d.ID, cell))
	}

	return res
}

type dashboardCellResponse struct {
	influxdb.Cell
	Properties influxdb.ViewProperties `json:"-"`
	Name       string                  `json:"name,omitempty"`
	Links      map[string]string       `json:"links"`
}

func (d *dashboardCellResponse) MarshalJSON() ([]byte, error) {
	r := struct {
		influxdb.Cell
		Properties json.RawMessage   `json:"properties,omitempty"`
		Name       string            `json:"name,omitempty"`
		Links      map[string]string `json:"links"`
	}{
		Cell:  d.Cell,
		Links: d.Links,
	}

	if d.Cell.View != nil {
		b, err := influxdb.MarshalViewPropertiesJSON(d.Cell.View.Properties)
		if err != nil {
			return nil, err
		}
		r.Properties = b
		r.Name = d.Cell.View.Name
	}

	return json.Marshal(r)
}

func (c dashboardCellResponse) toinfluxdb() *influxdb.Cell {
	return &c.Cell
}

func newDashboardCellResponse(dashboardID influxdb.ID, c *influxdb.Cell) dashboardCellResponse {
	resp := dashboardCellResponse{
		Cell: *c,
		Links: map[string]string{
			"self": fmt.Sprintf("/api/v2/dashboards/%s/cells/%s", dashboardID, c.ID),
			"view": fmt.Sprintf("/api/v2/dashboards/%s/cells/%s/view", dashboardID, c.ID),
		},
	}

	if c.View != nil {
		resp.Properties = c.View.Properties
		resp.Name = c.View.Name
	}
	return resp
}

type dashboardCellsResponse struct {
	Cells []dashboardCellResponse `json:"cells"`
	Links map[string]string       `json:"links"`
}

func newDashboardCellsResponse(dashboardID influxdb.ID, cs []*influxdb.Cell) dashboardCellsResponse {
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

type viewLinks struct {
	Self string `json:"self"`
}

type dashboardCellViewResponse struct {
	influxdb.View
	Links viewLinks `json:"links"`
}

func (r dashboardCellViewResponse) MarshalJSON() ([]byte, error) {
	props, err := influxdb.MarshalViewPropertiesJSON(r.Properties)
	if err != nil {
		return nil, err
	}

	return json.Marshal(struct {
		influxdb.ViewContents
		Links      viewLinks       `json:"links"`
		Properties json.RawMessage `json:"properties"`
	}{
		ViewContents: r.ViewContents,
		Links:        r.Links,
		Properties:   props,
	})
}

func newDashboardCellViewResponse(dashID, cellID influxdb.ID, v *influxdb.View) dashboardCellViewResponse {
	return dashboardCellViewResponse{
		Links: viewLinks{
			Self: fmt.Sprintf("/api/v2/dashboards/%s/cells/%s", dashID, cellID),
		},
		View: *v,
	}
}

// handleGetDashboards returns all dashboards within the store.
func (h *DashboardHandler) handleGetDashboards(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetDashboardsRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if req.ownerID != nil {
		filter := influxdb.UserResourceMappingFilter{
			UserID:       *req.ownerID,
			UserType:     influxdb.Owner,
			ResourceType: influxdb.DashboardsResourceType,
		}

		mappings, _, err := h.UserResourceMappingService.FindUserResourceMappings(ctx, filter)
		if err != nil {
			h.HandleHTTPError(ctx, &influxdb.Error{
				Code: influxdb.EInternal,
				Msg:  "Error loading dashboard owners",
				Err:  err,
			}, w)
			return
		}

		for _, mapping := range mappings {
			req.filter.IDs = append(req.filter.IDs, &mapping.ResourceID)
		}
	}

	dashboards, _, err := h.DashboardService.FindDashboards(ctx, req.filter, req.opts)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Dashboards retrieved", zap.String("dashboards", fmt.Sprint(dashboards)))

	if err := encodeResponse(ctx, w, http.StatusOK, newGetDashboardsResponse(ctx, dashboards, req.filter, req.opts, h.LabelService)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getDashboardsRequest struct {
	filter  influxdb.DashboardFilter
	opts    influxdb.FindOptions
	ownerID *influxdb.ID
}

func decodeGetDashboardsRequest(ctx context.Context, r *http.Request) (*getDashboardsRequest, error) {
	qp := r.URL.Query()
	req := &getDashboardsRequest{}

	opts, err := influxdb.DecodeFindOptions(r)
	if err != nil {
		return nil, err
	}
	req.opts = *opts

	initialID := influxdb.InvalidID()
	if ids, ok := qp["id"]; ok {
		for _, id := range ids {
			i := initialID
			if err := i.DecodeFromString(id); err != nil {
				return nil, err
			}
			req.filter.IDs = append(req.filter.IDs, &i)
		}
	} else if ownerID := qp.Get("ownerID"); ownerID != "" {
		req.ownerID = &initialID
		if err := req.ownerID.DecodeFromString(ownerID); err != nil {
			return nil, err
		}
	} else if orgID := qp.Get("orgID"); orgID != "" {
		id := influxdb.InvalidID()
		if err := id.DecodeFromString(orgID); err != nil {
			return nil, err
		}
		req.filter.OrganizationID = &id
	} else if org := qp.Get("org"); org != "" {
		req.filter.Organization = &org
	}

	return req, nil
}

type getDashboardsResponse struct {
	Links      *influxdb.PagingLinks `json:"links"`
	Dashboards []dashboardResponse   `json:"dashboards"`
}

func (d getDashboardsResponse) toinfluxdb() []*influxdb.Dashboard {
	res := make([]*influxdb.Dashboard, len(d.Dashboards))
	for i := range d.Dashboards {
		res[i] = d.Dashboards[i].toinfluxdb()
	}
	return res
}

func newGetDashboardsResponse(ctx context.Context, dashboards []*influxdb.Dashboard, filter influxdb.DashboardFilter, opts influxdb.FindOptions, labelService influxdb.LabelService) getDashboardsResponse {
	res := getDashboardsResponse{
		Links:      influxdb.NewPagingLinks(prefixDashboards, opts, filter, len(dashboards)),
		Dashboards: make([]dashboardResponse, 0, len(dashboards)),
	}

	for _, dashboard := range dashboards {
		if dashboard != nil {
			labels, _ := labelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: dashboard.ID, ResourceType: influxdb.DashboardsResourceType})
			res.Dashboards = append(res.Dashboards, newDashboardResponse(dashboard, labels))
		}
	}

	return res
}

// handlePostDashboard creates a new dashboard.
func (h *DashboardHandler) handlePostDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var d influxdb.Dashboard
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.DashboardService.CreateDashboard(ctx, &d); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newDashboardResponse(&d, []*influxdb.Label{})); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// handleGetDashboard retrieves a dashboard by ID.
func (h *DashboardHandler) handleGetDashboard(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetDashboardRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	dashboard, err := h.DashboardService.FindDashboardByID(ctx, req.DashboardID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if r.URL.Query().Get("include") == "properties" {
		for _, c := range dashboard.Cells {
			view, err := h.DashboardService.GetDashboardCellView(ctx, dashboard.ID, c.ID)
			if err != nil {
				h.HandleHTTPError(ctx, err, w)
				return
			}

			if view != nil {
				c.View = view
			}
		}
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: dashboard.ID, ResourceType: influxdb.DashboardsResourceType})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Dashboard retrieved", zap.String("dashboard", fmt.Sprint(dashboard)))

	if err := encodeResponse(ctx, w, http.StatusOK, newDashboardResponse(dashboard, labels)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getDashboardRequest struct {
	DashboardID influxdb.ID
}

func decodeGetDashboardRequest(ctx context.Context, r *http.Request) (*getDashboardRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i influxdb.ID
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
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.DashboardService.DeleteDashboard(ctx, req.DashboardID); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Dashboard deleted", zap.String("dashboardID", req.DashboardID.String()))

	w.WriteHeader(http.StatusNoContent)
}

type deleteDashboardRequest struct {
	DashboardID influxdb.ID
}

func decodeDeleteDashboardRequest(ctx context.Context, r *http.Request) (*deleteDashboardRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i influxdb.ID
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
		h.HandleHTTPError(ctx, err, w)
		return
	}
	dashboard, err := h.DashboardService.UpdateDashboard(ctx, req.DashboardID, req.Upd)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: dashboard.ID, ResourceType: influxdb.DashboardsResourceType})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Dashboard updated", zap.String("dashboard", fmt.Sprint(dashboard)))

	if err := encodeResponse(ctx, w, http.StatusOK, newDashboardResponse(dashboard, labels)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type patchDashboardRequest struct {
	DashboardID influxdb.ID
	Upd         influxdb.DashboardUpdate
}

func decodePatchDashboardRequest(ctx context.Context, r *http.Request) (*patchDashboardRequest, error) {
	req := &patchDashboardRequest{}
	upd := influxdb.DashboardUpdate{}
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	req.Upd = upd

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}
	var i influxdb.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}

	req.DashboardID = i

	if err := req.Valid(); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	return req, nil
}

// Valid validates that the dashboard ID is non zero valued and update has expected values set.
func (r *patchDashboardRequest) Valid() error {
	if !r.DashboardID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "missing dashboard ID",
		}
	}

	if pe := r.Upd.Valid(); pe != nil {
		return pe
	}
	return nil
}

type postDashboardCellRequest struct {
	dashboardID influxdb.ID
	*influxdb.CellProperty
	UsingView *influxdb.ID `json:"usingView"`
	Name      *string      `json:"name"`
}

func decodePostDashboardCellRequest(ctx context.Context, r *http.Request) (*postDashboardCellRequest, error) {
	req := &postDashboardCellRequest{}
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "bad request json body",
			Err:  err,
		}
	}

	if err := req.dashboardID.DecodeFromString(id); err != nil {
		return nil, err
	}

	return req, nil
}

// handlePostDashboardCell creates a dashboard cell.
func (h *DashboardHandler) handlePostDashboardCell(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePostDashboardCellRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	cell := new(influxdb.Cell)

	opts := new(influxdb.AddDashboardCellOptions)
	if req.UsingView != nil || req.Name != nil {
		opts.View = new(influxdb.View)
		if req.UsingView != nil {
			// load the view
			opts.View, err = h.DashboardService.GetDashboardCellView(ctx, req.dashboardID, *req.UsingView)
			if err != nil {
				h.HandleHTTPError(ctx, err, w)
				return
			}
		}
		if req.Name != nil {
			opts.View.Name = *req.Name
		}
	} else if req.CellProperty == nil {
		h.HandleHTTPError(ctx, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "req body is empty",
		}, w)
		return
	}

	if req.CellProperty != nil {
		cell.CellProperty = *req.CellProperty
	}

	if err := h.DashboardService.AddDashboardCell(ctx, req.dashboardID, cell, *opts); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Dashboard cell created", zap.String("dashboardID", req.dashboardID.String()), zap.String("cell", fmt.Sprint(cell)))

	if err := encodeResponse(ctx, w, http.StatusCreated, newDashboardCellResponse(req.dashboardID, cell)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type putDashboardCellRequest struct {
	dashboardID influxdb.ID
	cells       []*influxdb.Cell
}

func decodePutDashboardCellRequest(ctx context.Context, r *http.Request) (*putDashboardCellRequest, error) {
	req := &putDashboardCellRequest{}

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}
	if err := req.dashboardID.DecodeFromString(id); err != nil {
		return nil, err
	}

	req.cells = []*influxdb.Cell{}
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
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.DashboardService.ReplaceDashboardCells(ctx, req.dashboardID, req.cells); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Dashboard cell replaced", zap.String("dashboardID", req.dashboardID.String()), zap.String("cells", fmt.Sprint(req.cells)))

	if err := encodeResponse(ctx, w, http.StatusCreated, newDashboardCellsResponse(req.dashboardID, req.cells)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type deleteDashboardCellRequest struct {
	dashboardID influxdb.ID
	cellID      influxdb.ID
}

func decodeDeleteDashboardCellRequest(ctx context.Context, r *http.Request) (*deleteDashboardCellRequest, error) {
	req := &deleteDashboardCellRequest{}

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}
	if err := req.dashboardID.DecodeFromString(id); err != nil {
		return nil, err
	}

	cellID := params.ByName("cellID")
	if cellID == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing cellID",
		}
	}
	if err := req.cellID.DecodeFromString(cellID); err != nil {
		return nil, err
	}

	return req, nil
}

type getDashboardCellViewRequest struct {
	dashboardID influxdb.ID
	cellID      influxdb.ID
}

func decodeGetDashboardCellViewRequest(ctx context.Context, r *http.Request) (*getDashboardCellViewRequest, error) {
	req := &getDashboardCellViewRequest{}

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, influxdb.NewError(influxdb.WithErrorMsg("url missing id"), influxdb.WithErrorCode(influxdb.EInvalid))
	}
	if err := req.dashboardID.DecodeFromString(id); err != nil {
		return nil, err
	}

	cellID := params.ByName("cellID")
	if cellID == "" {
		return nil, influxdb.NewError(influxdb.WithErrorMsg("url missing cellID"), influxdb.WithErrorCode(influxdb.EInvalid))
	}
	if err := req.cellID.DecodeFromString(cellID); err != nil {
		return nil, err
	}

	return req, nil
}

func (h *DashboardHandler) handleGetDashboardCellView(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetDashboardCellViewRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	view, err := h.DashboardService.GetDashboardCellView(ctx, req.dashboardID, req.cellID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Dashboard cell view retrieved", zap.String("dashboardID", req.dashboardID.String()), zap.String("cellID", req.cellID.String()), zap.String("view", fmt.Sprint(view)))

	if err := encodeResponse(ctx, w, http.StatusOK, newDashboardCellViewResponse(req.dashboardID, req.cellID, view)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type patchDashboardCellViewRequest struct {
	dashboardID influxdb.ID
	cellID      influxdb.ID
	upd         influxdb.ViewUpdate
}

func decodePatchDashboardCellViewRequest(ctx context.Context, r *http.Request) (*patchDashboardCellViewRequest, error) {
	req := &patchDashboardCellViewRequest{}

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, influxdb.NewError(influxdb.WithErrorMsg("url missing id"), influxdb.WithErrorCode(influxdb.EInvalid))
	}
	if err := req.dashboardID.DecodeFromString(id); err != nil {
		return nil, err
	}

	cellID := params.ByName("cellID")
	if cellID == "" {
		return nil, influxdb.NewError(influxdb.WithErrorMsg("url missing cellID"), influxdb.WithErrorCode(influxdb.EInvalid))
	}
	if err := req.cellID.DecodeFromString(cellID); err != nil {
		return nil, err
	}

	if err := json.NewDecoder(r.Body).Decode(&req.upd); err != nil {
		return nil, err
	}

	return req, nil
}

func (h *DashboardHandler) handlePatchDashboardCellView(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePatchDashboardCellViewRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	view, err := h.DashboardService.UpdateDashboardCellView(ctx, req.dashboardID, req.cellID, req.upd)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Dashboard cell view updated", zap.String("dashboardID", req.dashboardID.String()), zap.String("cellID", req.cellID.String()), zap.String("view", fmt.Sprint(view)))

	if err := encodeResponse(ctx, w, http.StatusOK, newDashboardCellViewResponse(req.dashboardID, req.cellID, view)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// handleDeleteDashboardCell deletes a dashboard cell.
func (h *DashboardHandler) handleDeleteDashboardCell(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeDeleteDashboardCellRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	if err := h.DashboardService.RemoveDashboardCell(ctx, req.dashboardID, req.cellID); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Dashboard cell deleted", zap.String("dashboardID", req.dashboardID.String()), zap.String("cellID", req.cellID.String()))

	w.WriteHeader(http.StatusNoContent)
}

type patchDashboardCellRequest struct {
	dashboardID influxdb.ID
	cellID      influxdb.ID
	upd         influxdb.CellUpdate
}

func decodePatchDashboardCellRequest(ctx context.Context, r *http.Request) (*patchDashboardCellRequest, error) {
	req := &patchDashboardCellRequest{}

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}
	if err := req.dashboardID.DecodeFromString(id); err != nil {
		return nil, err
	}

	cellID := params.ByName("cellID")
	if cellID == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "cannot provide empty cell id",
		}
	}
	if err := req.cellID.DecodeFromString(cellID); err != nil {
		return nil, err
	}

	if err := json.NewDecoder(r.Body).Decode(&req.upd); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	if pe := req.upd.Valid(); pe != nil {
		return nil, pe
	}

	return req, nil
}

// handlePatchDashboardCell updates a dashboard cell.
func (h *DashboardHandler) handlePatchDashboardCell(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePatchDashboardCellRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	cell, err := h.DashboardService.UpdateDashboardCell(ctx, req.dashboardID, req.cellID, req.upd)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	h.log.Debug("Dashboard cell updated", zap.String("dashboardID", req.dashboardID.String()), zap.String("cell", fmt.Sprint(cell)))

	if err := encodeResponse(ctx, w, http.StatusOK, newDashboardCellResponse(req.dashboardID, cell)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// DashboardService is a dashboard service over HTTP to the influxdb server.
type DashboardService struct {
	Client *httpc.Client
}

// FindDashboardByID returns a single dashboard by ID.
func (s *DashboardService) FindDashboardByID(ctx context.Context, id influxdb.ID) (*influxdb.Dashboard, error) {
	var dr dashboardResponse
	err := s.Client.
		Get(prefixDashboards, id.String()).
		QueryParams([2]string{"include", "properties"}).
		DecodeJSON(&dr).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return dr.toinfluxdb(), nil
}

// FindDashboards returns a list of dashboards that match filter and the total count of matching dashboards.
// Additional options provide pagination & sorting.
func (s *DashboardService) FindDashboards(ctx context.Context, filter influxdb.DashboardFilter, opts influxdb.FindOptions) ([]*influxdb.Dashboard, int, error) {
	queryPairs := influxdb.FindOptionParams(opts)
	for _, id := range filter.IDs {
		queryPairs = append(queryPairs, [2]string{"id", id.String()})
	}
	if filter.OrganizationID != nil {
		queryPairs = append(queryPairs, [2]string{"orgID", filter.OrganizationID.String()})
	}
	if filter.Organization != nil {
		queryPairs = append(queryPairs, [2]string{"org", *filter.Organization})
	}

	var dr getDashboardsResponse
	err := s.Client.
		Get(prefixDashboards).
		QueryParams(queryPairs...).
		DecodeJSON(&dr).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	dashboards := dr.toinfluxdb()
	return dashboards, len(dashboards), nil
}

// CreateDashboard creates a new dashboard and sets b.ID with the new identifier.
func (s *DashboardService) CreateDashboard(ctx context.Context, d *influxdb.Dashboard) error {
	return s.Client.
		PostJSON(d, prefixDashboards).
		DecodeJSON(d).
		Do(ctx)
}

// UpdateDashboard updates a single dashboard with changeset.
// Returns the new dashboard state after update.
func (s *DashboardService) UpdateDashboard(ctx context.Context, id influxdb.ID, upd influxdb.DashboardUpdate) (*influxdb.Dashboard, error) {
	var d influxdb.Dashboard
	err := s.Client.
		PatchJSON(upd, prefixDashboards, id.String()).
		DecodeJSON(&d).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	if len(d.Cells) == 0 {
		// TODO(@jsteenb2): decipher why this is doing this?
		d.Cells = nil
	}

	return &d, nil
}

// DeleteDashboard removes a dashboard by ID.
func (s *DashboardService) DeleteDashboard(ctx context.Context, id influxdb.ID) error {
	return s.Client.
		Delete(dashboardIDPath(id)).
		Do(ctx)
}

// AddDashboardCell adds a cell to a dashboard.
func (s *DashboardService) AddDashboardCell(ctx context.Context, id influxdb.ID, c *influxdb.Cell, opts influxdb.AddDashboardCellOptions) error {
	return s.Client.
		PostJSON(c, cellPath(id)).
		DecodeJSON(c).
		Do(ctx)
}

// RemoveDashboardCell removes a dashboard.
func (s *DashboardService) RemoveDashboardCell(ctx context.Context, dashboardID, cellID influxdb.ID) error {
	return s.Client.
		Delete(dashboardCellIDPath(dashboardID, cellID)).
		Do(ctx)
}

// UpdateDashboardCell replaces the dashboard cell with the provided ID.
func (s *DashboardService) UpdateDashboardCell(ctx context.Context, dashboardID, cellID influxdb.ID, upd influxdb.CellUpdate) (*influxdb.Cell, error) {
	if err := upd.Valid(); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	var c influxdb.Cell
	err := s.Client.
		PatchJSON(upd, dashboardCellIDPath(dashboardID, cellID)).
		DecodeJSON(&c).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

// GetDashboardCellView retrieves the view for a dashboard cell.
func (s *DashboardService) GetDashboardCellView(ctx context.Context, dashboardID, cellID influxdb.ID) (*influxdb.View, error) {
	var dcv dashboardCellViewResponse
	err := s.Client.
		Get(cellViewPath(dashboardID, cellID)).
		DecodeJSON(&dcv).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return &dcv.View, nil
}

// UpdateDashboardCellView updates the view for a dashboard cell.
func (s *DashboardService) UpdateDashboardCellView(ctx context.Context, dashboardID, cellID influxdb.ID, upd influxdb.ViewUpdate) (*influxdb.View, error) {
	var dcv dashboardCellViewResponse
	err := s.Client.
		PatchJSON(upd, cellViewPath(dashboardID, cellID)).
		DecodeJSON(&dcv).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &dcv.View, nil
}

// ReplaceDashboardCells replaces all cells in a dashboard
func (s *DashboardService) ReplaceDashboardCells(ctx context.Context, id influxdb.ID, cs []*influxdb.Cell) error {
	return s.Client.
		PutJSON(cs, cellPath(id)).
		// TODO: previous implementation did not do anything with the response except validate it is valid json.
		//  seems likely we should have to overwrite (:sadpanda:) the incoming cs...
		DecodeJSON(&dashboardCellsResponse{}).
		Do(ctx)
}

func dashboardIDPath(id influxdb.ID) string {
	return path.Join(prefixDashboards, id.String())
}

func cellPath(id influxdb.ID) string {
	return path.Join(dashboardIDPath(id), "cells")
}

func cellViewPath(dashboardID, cellID influxdb.ID) string {
	return path.Join(dashboardCellIDPath(dashboardID, cellID), "view")
}

func dashboardCellIDPath(id influxdb.ID, cellID influxdb.ID) string {
	return path.Join(cellPath(id), cellID.String())
}
