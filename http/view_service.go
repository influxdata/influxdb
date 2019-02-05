// NOTE: This service has been deprecated and should not be used.
// Views are now resources that belong to dashboards. The reason for
// this is due to how we authorize operations against views.
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/influxdb"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// ViewBackend is all services and associated parameters required to construct
// the ScraperHandler.
type ViewBackend struct {
	Logger *zap.Logger

	ViewService                influxdb.ViewService
	UserService                influxdb.UserService
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
}

// NewViewBackend returns a new instance of ViewBackend.
func NewViewBackend(b *APIBackend) *ViewBackend {
	return &ViewBackend{
		Logger: b.Logger.With(zap.String("handler", "scraper")),

		ViewService:  b.ViewService,
		UserService:  b.UserService,
		LabelService: b.LabelService,
	}
}

// ViewHandler is the handler for the view service
type ViewHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	ViewService                influxdb.ViewService
	UserService                influxdb.UserService
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
}

const (
	viewsPath            = "/api/v2/views"
	viewsIDPath          = "/api/v2/views/:id"
	viewsIDMembersPath   = "/api/v2/views/:id/members"
	viewsIDMembersIDPath = "/api/v2/views/:id/members/:userID"
	viewsIDOwnersPath    = "/api/v2/views/:id/owners"
	viewsIDOwnersIDPath  = "/api/v2/views/:id/owners/:userID"
	viewsIDLabelsPath    = "/api/v2/views/:id/labels"
	viewsIDLabelsIDPath  = "/api/v2/views/:id/labels/:lid"
)

// NewViewHandler returns a new instance of ViewHandler.
func NewViewHandler(b *ViewBackend) *ViewHandler {
	h := &ViewHandler{
		Router: NewRouter(),
		Logger: b.Logger,

		ViewService:                b.ViewService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
	}

	h.HandlerFunc("POST", viewsPath, h.handlePostViews)
	h.HandlerFunc("GET", viewsPath, h.handleGetViews)

	h.HandlerFunc("GET", viewsIDPath, h.handleGetView)
	h.HandlerFunc("DELETE", viewsIDPath, h.handleDeleteView)
	h.HandlerFunc("PATCH", viewsIDPath, h.handlePatchView)

	memberBackend := MemberBackend{
		Logger:                     b.Logger.With(zap.String("handler", "member")),
		ResourceType:               influxdb.ViewsResourceType,
		UserType:                   influxdb.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", viewsIDMembersPath, newPostMemberHandler(memberBackend))
	h.HandlerFunc("GET", viewsIDMembersPath, newGetMembersHandler(memberBackend))
	h.HandlerFunc("DELETE", viewsIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		Logger:                     b.Logger.With(zap.String("handler", "member")),
		ResourceType:               influxdb.ViewsResourceType,
		UserType:                   influxdb.Owner,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", viewsIDOwnersPath, newPostMemberHandler(ownerBackend))
	h.HandlerFunc("GET", viewsIDOwnersPath, newGetMembersHandler(ownerBackend))
	h.HandlerFunc("DELETE", viewsIDOwnersIDPath, newDeleteMemberHandler(ownerBackend))

	labelBackend := &LabelBackend{
		Logger:       b.Logger.With(zap.String("handler", "label")),
		LabelService: b.LabelService,
	}
	h.HandlerFunc("GET", viewsIDLabelsPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", viewsIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", viewsIDLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

type viewLinks struct {
	Self   string `json:"self"`
	Labels string `json:"labels"`
}

type viewResponse struct {
	influxdb.View
	Links viewLinks `json:"links"`
}

func (r viewResponse) MarshalJSON() ([]byte, error) {
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

func newViewResponse(c *influxdb.View) viewResponse {
	return viewResponse{
		Links: viewLinks{
			Self:   fmt.Sprintf("/api/v2/views/%s", c.ID),
			Labels: fmt.Sprintf("/api/v2/views/%s/labels", c.ID),
		},
		View: *c,
	}
}

// handleGetViews returns all views within the store.
func (h *ViewHandler) handleGetViews(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req := decodeGetViewsRequest(ctx, r)

	views, _, err := h.ViewService.FindViews(ctx, req.filter)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newGetViewsResponse(views)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type getViewsRequest struct {
	filter influxdb.ViewFilter
}

func decodeGetViewsRequest(ctx context.Context, r *http.Request) *getViewsRequest {
	qp := r.URL.Query()

	return &getViewsRequest{
		filter: influxdb.ViewFilter{
			Types: qp["type"],
		},
	}
}

type getViewsLinks struct {
	Self string `json:"self"`
}

type getViewsResponse struct {
	Links getViewsLinks  `json:"links"`
	Views []viewResponse `json:"views"`
}

func newGetViewsResponse(views []*influxdb.View) getViewsResponse {
	res := getViewsResponse{
		Links: getViewsLinks{
			Self: "/api/v2/views",
		},
		Views: make([]viewResponse, 0, len(views)),
	}

	for _, view := range views {
		res.Views = append(res.Views, newViewResponse(view))
	}

	return res
}

// handlePostViews creates a new view.
func (h *ViewHandler) handlePostViews(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostViewRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	if err := h.ViewService.CreateView(ctx, req.View); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newViewResponse(req.View)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type postViewRequest struct {
	View *influxdb.View
}

func decodePostViewRequest(ctx context.Context, r *http.Request) (*postViewRequest, error) {
	c := &influxdb.View{}
	if err := json.NewDecoder(r.Body).Decode(c); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  err.Error(),
		}
	}
	return &postViewRequest{
		View: c,
	}, nil
}

// hanldeGetView retrieves a view by ID.
func (h *ViewHandler) handleGetView(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetViewRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	view, err := h.ViewService.FindViewByID(ctx, req.ViewID)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newViewResponse(view)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type getViewRequest struct {
	ViewID influxdb.ID
}

func decodeGetViewRequest(ctx context.Context, r *http.Request) (*getViewRequest, error) {
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

	return &getViewRequest{
		ViewID: i,
	}, nil
}

// handleDeleteView removes a view by ID.
func (h *ViewHandler) handleDeleteView(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeDeleteViewRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := h.ViewService.DeleteView(ctx, req.ViewID); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type deleteViewRequest struct {
	ViewID influxdb.ID
}

func decodeDeleteViewRequest(ctx context.Context, r *http.Request) (*deleteViewRequest, error) {
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

	return &deleteViewRequest{
		ViewID: i,
	}, nil
}

// handlePatchView updates a view.
func (h *ViewHandler) handlePatchView(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, pe := decodePatchViewRequest(ctx, r)
	if pe != nil {
		EncodeError(ctx, pe, w)
		return
	}
	view, err := h.ViewService.UpdateView(ctx, req.ViewID, req.Upd)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newViewResponse(view)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

type patchViewRequest struct {
	ViewID influxdb.ID
	Upd    influxdb.ViewUpdate
}

func decodePatchViewRequest(ctx context.Context, r *http.Request) (*patchViewRequest, *influxdb.Error) {
	req := &patchViewRequest{}
	upd := influxdb.ViewUpdate{}
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  err.Error(),
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
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	req.ViewID = i

	if err := req.Valid(); err != nil {
		return nil, &influxdb.Error{
			Err: err,
		}
	}

	return req, nil
}

// Valid validates that the view ID is non zero valued and update has expected values set.
func (r *patchViewRequest) Valid() *influxdb.Error {
	if !r.ViewID.Valid() {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "missing view ID",
		}
	}

	return r.Upd.Valid()
}
