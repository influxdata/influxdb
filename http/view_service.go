package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/kit/errors"
	"github.com/julienschmidt/httprouter"
)

// ViewHandler is the handler for the view service
type ViewHandler struct {
	*httprouter.Router

	ViewService platform.ViewService
}

// NewViewHandler returns a new instance of ViewHandler.
func NewViewHandler() *ViewHandler {
	h := &ViewHandler{
		Router: httprouter.New(),
	}

	h.HandlerFunc("POST", "/v2/views", h.handlePostViews)
	h.HandlerFunc("GET", "/v2/views", h.handleGetViews)
	h.HandlerFunc("GET", "/v2/views/:id", h.handleGetView)
	h.HandlerFunc("DELETE", "/v2/views/:id", h.handleDeleteView)
	h.HandlerFunc("PATCH", "/v2/views/:id", h.handlePatchView)
	return h
}

type viewLinks struct {
	Self string `json:"self"`
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
			Self: fmt.Sprintf("/v2/views/%s", c.ID),
		},
		View: *c,
	}
}

// handleGetViews returns all views within the store.
func (h *ViewHandler) handleGetViews(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// TODO(desa): support filtering via query params
	views, _, err := h.ViewService.FindViews(ctx, platform.ViewFilter{})
	if err != nil {
		EncodeError(ctx, errors.InternalErrorf("Error loading views: %v", err), w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newGetViewsResponse(views)); err != nil {
		EncodeError(ctx, err, w)
		return
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
			Self: "/v2/views",
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
		EncodeError(ctx, errors.InternalErrorf("Error loading views: %v", err), w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, newViewResponse(req.View)); err != nil {
		EncodeError(ctx, err, w)
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
		if err == platform.ErrViewNotFound {
			err = errors.New(err.Error(), errors.NotFound)
		}
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newViewResponse(view)); err != nil {
		EncodeError(ctx, err, w)
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
		return nil, errors.InvalidDataf("url missing id")
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
		if err == platform.ErrViewNotFound {
			err = errors.New(err.Error(), errors.NotFound)
		}
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
		return nil, errors.InvalidDataf("url missing id")
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

	req, err := decodePatchViewRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}
	view, err := h.ViewService.UpdateView(ctx, req.ViewID, req.Upd)
	if err != nil {
		if err == platform.ErrViewNotFound {
			err = errors.New(err.Error(), errors.NotFound)
		}
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newViewResponse(view)); err != nil {
		EncodeError(ctx, err, w)
		return
	}
}

type patchViewRequest struct {
	ViewID platform.ID
	Upd    platform.ViewUpdate
}

func decodePatchViewRequest(ctx context.Context, r *http.Request) (*patchViewRequest, error) {
	req := &patchViewRequest{}
	upd := platform.ViewUpdate{}
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

	req.ViewID = i

	if err := req.Valid(); err != nil {
		return nil, errors.MalformedDataf(err.Error())
	}

	return req, nil
}

// Valid validates that the view ID is non zero valued and update has expected values set.
func (r *patchViewRequest) Valid() error {
	if len(r.ViewID) == 0 {
		return fmt.Errorf("missing view ID")
	}

	return r.Upd.Valid()
}
