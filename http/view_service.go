// NOTE: This service has been deprecated and should not be used.
// Views are now resources that belong to dashboards. The reason for
// this is due to how we authorize operations against views.
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	platform "github.com/influxdata/influxdb"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// ViewHandler is the handler for the view service
type ViewHandler struct {
	*httprouter.Router

	Logger *zap.Logger

	ViewService                platform.ViewService
	UserResourceMappingService platform.UserResourceMappingService
	LabelService               platform.LabelService
	UserService                platform.UserService
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
func NewViewHandler(mappingService platform.UserResourceMappingService, labelService platform.LabelService, userService platform.UserService) *ViewHandler {
	h := &ViewHandler{
		Router: NewRouter(),
		Logger: zap.NewNop(),

		UserResourceMappingService: mappingService,
		LabelService:               labelService,
		UserService:                userService,
	}

	h.HandlerFunc("POST", viewsPath, h.handlePostViews)
	h.HandlerFunc("GET", viewsPath, h.handleGetViews)

	h.HandlerFunc("GET", viewsIDPath, h.handleGetView)
	h.HandlerFunc("DELETE", viewsIDPath, h.handleDeleteView)
	h.HandlerFunc("PATCH", viewsIDPath, h.handlePatchView)

	h.HandlerFunc("POST", viewsIDMembersPath, newPostMemberHandler(h.UserResourceMappingService, h.UserService, platform.DashboardsResourceType, platform.Member))
	h.HandlerFunc("GET", viewsIDMembersPath, newGetMembersHandler(h.UserResourceMappingService, h.UserService, platform.DashboardsResourceType, platform.Member))
	h.HandlerFunc("DELETE", viewsIDMembersIDPath, newDeleteMemberHandler(h.UserResourceMappingService, platform.Member))

	h.HandlerFunc("POST", viewsIDOwnersPath, newPostMemberHandler(h.UserResourceMappingService, h.UserService, platform.DashboardsResourceType, platform.Owner))
	h.HandlerFunc("GET", viewsIDOwnersPath, newGetMembersHandler(h.UserResourceMappingService, h.UserService, platform.DashboardsResourceType, platform.Owner))
	h.HandlerFunc("DELETE", viewsIDOwnersIDPath, newDeleteMemberHandler(h.UserResourceMappingService, platform.Owner))

	h.HandlerFunc("GET", viewsIDLabelsPath, newGetLabelsHandler(h.LabelService))
	h.HandlerFunc("POST", viewsIDLabelsPath, newPostLabelHandler(h.LabelService))
	h.HandlerFunc("DELETE", viewsIDLabelsIDPath, newDeleteLabelHandler(h.LabelService))

	return h
}

type viewLinks struct {
	Self   string `json:"self"`
	Labels string `json:"labels"`
}

type viewResponse struct {
	platform.View
	Links viewLinks `json:"links"`
}

func (r viewResponse) MarshalJSON() ([]byte, error) {
	props, err := platform.MarshalViewPropertiesJSON(r.Properties)
	if err != nil {
		return nil, err
	}

	return json.Marshal(struct {
		platform.ViewContents
		Links      viewLinks       `json:"links"`
		Properties json.RawMessage `json:"properties"`
	}{
		ViewContents: r.ViewContents,
		Links:        r.Links,
		Properties:   props,
	})
}

func newViewResponse(c *platform.View) viewResponse {
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
	filter platform.ViewFilter
}

func decodeGetViewsRequest(ctx context.Context, r *http.Request) *getViewsRequest {
	qp := r.URL.Query()

	return &getViewsRequest{
		filter: platform.ViewFilter{
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

func newGetViewsResponse(views []*platform.View) getViewsResponse {
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
	View *platform.View
}

func decodePostViewRequest(ctx context.Context, r *http.Request) (*postViewRequest, error) {
	c := &platform.View{}
	if err := json.NewDecoder(r.Body).Decode(c); err != nil {
		return nil, err
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
	ViewID platform.ID
}

func decodeGetViewRequest(ctx context.Context, r *http.Request) (*getViewRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i platform.ID
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
	ViewID platform.ID
}

func decodeDeleteViewRequest(ctx context.Context, r *http.Request) (*deleteViewRequest, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "url missing id",
		}
	}

	var i platform.ID
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
	ViewID platform.ID
	Upd    platform.ViewUpdate
}

func decodePatchViewRequest(ctx context.Context, r *http.Request) (*patchViewRequest, *platform.Error) {
	req := &patchViewRequest{}
	upd := platform.ViewUpdate{}
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Err:  err,
		}
	}

	req.Upd = upd

	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
			Msg:  "url missing id",
		}
	}
	var i platform.ID
	if err := i.DecodeFromString(id); err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	req.ViewID = i

	if err := req.Valid(); err != nil {
		return nil, &platform.Error{
			Err: err,
		}
	}

	return req, nil
}

// Valid validates that the view ID is non zero valued and update has expected values set.
func (r *patchViewRequest) Valid() *platform.Error {
	if !r.ViewID.Valid() {
		return &platform.Error{
			Code: platform.EInvalid,
			Msg:  "missing view ID",
		}
	}

	return r.Upd.Valid()
}
