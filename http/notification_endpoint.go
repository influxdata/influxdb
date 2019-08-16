package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/influxdb"
	pctx "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/notification/endpoint"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// NotificationEndpointBackend is all services and associated parameters required to construct
// the NotificationEndpointBackendHandler.
type NotificationEndpointBackend struct {
	influxdb.HTTPErrorHandler
	Logger *zap.Logger

	NotificationEndpointService influxdb.NotificationEndpointService
	UserResourceMappingService  influxdb.UserResourceMappingService
	LabelService                influxdb.LabelService
	UserService                 influxdb.UserService
	OrganizationService         influxdb.OrganizationService
}

// NewNotificationEndpointBackend returns a new instance of NotificationEndpointBackend.
func NewNotificationEndpointBackend(b *APIBackend) *NotificationEndpointBackend {
	return &NotificationEndpointBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Logger:           b.Logger.With(zap.String("handler", "notificationEndpoint")),

		NotificationEndpointService: b.NotificationEndpointService,
		UserResourceMappingService:  b.UserResourceMappingService,
		LabelService:                b.LabelService,
		UserService:                 b.UserService,
		OrganizationService:         b.OrganizationService,
	}
}

// NotificationEndpointHandler is the handler for the notificationEndpoint service
type NotificationEndpointHandler struct {
	*httprouter.Router
	influxdb.HTTPErrorHandler
	Logger *zap.Logger

	NotificationEndpointService influxdb.NotificationEndpointService
	UserResourceMappingService  influxdb.UserResourceMappingService
	LabelService                influxdb.LabelService
	UserService                 influxdb.UserService
	OrganizationService         influxdb.OrganizationService
}

const (
	notificationEndpointsPath            = "/api/v2/notificationEndpoints"
	notificationEndpointsIDPath          = "/api/v2/notificationEndpoints/:id"
	notificationEndpointsIDMembersPath   = "/api/v2/notificationEndpoints/:id/members"
	notificationEndpointsIDMembersIDPath = "/api/v2/notificationEndpoints/:id/members/:userID"
	notificationEndpointsIDOwnersPath    = "/api/v2/notificationEndpoints/:id/owners"
	notificationEndpointsIDOwnersIDPath  = "/api/v2/notificationEndpoints/:id/owners/:userID"
	notificationEndpointsIDLabelsPath    = "/api/v2/notificationEndpoints/:id/labels"
	notificationEndpointsIDLabelsIDPath  = "/api/v2/notificationEndpoints/:id/labels/:lid"
)

// NewNotificationEndpointHandler returns a new instance of NotificationEndpointHandler.
func NewNotificationEndpointHandler(b *NotificationEndpointBackend) *NotificationEndpointHandler {
	h := &NotificationEndpointHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		Logger:           b.Logger,

		NotificationEndpointService: b.NotificationEndpointService,
		UserResourceMappingService:  b.UserResourceMappingService,
		LabelService:                b.LabelService,
		UserService:                 b.UserService,
		OrganizationService:         b.OrganizationService,
	}
	h.HandlerFunc("POST", notificationEndpointsPath, h.handlePostNotificationEndpoint)
	h.HandlerFunc("GET", notificationEndpointsPath, h.handleGetNotificationEndpoints)
	h.HandlerFunc("GET", notificationEndpointsIDPath, h.handleGetNotificationEndpoint)
	h.HandlerFunc("DELETE", notificationEndpointsIDPath, h.handleDeleteNotificationEndpoint)
	h.HandlerFunc("PUT", notificationEndpointsIDPath, h.handlePutNotificationEndpoint)
	h.HandlerFunc("PATCH", notificationEndpointsIDPath, h.handlePatchNotificationEndpoint)

	memberBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		Logger:                     b.Logger.With(zap.String("handler", "member")),
		ResourceType:               influxdb.NotificationEndpointResourceType,
		UserType:                   influxdb.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", notificationEndpointsIDMembersPath, newPostMemberHandler(memberBackend))
	h.HandlerFunc("GET", notificationEndpointsIDMembersPath, newGetMembersHandler(memberBackend))
	h.HandlerFunc("DELETE", notificationEndpointsIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		Logger:                     b.Logger.With(zap.String("handler", "member")),
		ResourceType:               influxdb.NotificationEndpointResourceType,
		UserType:                   influxdb.Owner,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", notificationEndpointsIDOwnersPath, newPostMemberHandler(ownerBackend))
	h.HandlerFunc("GET", notificationEndpointsIDOwnersPath, newGetMembersHandler(ownerBackend))
	h.HandlerFunc("DELETE", notificationEndpointsIDOwnersIDPath, newDeleteMemberHandler(ownerBackend))

	labelBackend := &LabelBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Logger:           b.Logger.With(zap.String("handler", "label")),
		LabelService:     b.LabelService,
		ResourceType:     influxdb.TelegrafsResourceType,
	}
	h.HandlerFunc("GET", notificationEndpointsIDLabelsIDPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", notificationEndpointsIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", notificationEndpointsIDLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

type notificationEndpointLinks struct {
	Self    string `json:"self"`
	Labels  string `json:"labels"`
	Members string `json:"members"`
	Owners  string `json:"owners"`
}

type notificationEndpointResponse struct {
	influxdb.NotificationEndpoint
	Labels []influxdb.Label          `json:"labels"`
	Links  notificationEndpointLinks `json:"links"`
}

func (resp notificationEndpointResponse) MarshalJSON() ([]byte, error) {
	b1, err := json.Marshal(resp.NotificationEndpoint)
	if err != nil {
		return nil, err
	}

	b2, err := json.Marshal(struct {
		Labels []influxdb.Label          `json:"labels"`
		Links  notificationEndpointLinks `json:"links"`
	}{
		Links:  resp.Links,
		Labels: resp.Labels,
	})
	if err != nil {
		return nil, err
	}

	return []byte(string(b1[:len(b1)-1]) + ", " + string(b2[1:])), nil
}

type notificationEndpointsResponse struct {
	NotificationEndpoints []*notificationEndpointResponse `json:"notificationEndpoints"`
	Links                 *influxdb.PagingLinks           `json:"links"`
}

func newNotificationEndpointResponse(edp influxdb.NotificationEndpoint, labels []*influxdb.Label) *notificationEndpointResponse {
	res := &notificationEndpointResponse{
		NotificationEndpoint: edp,
		Links: notificationEndpointLinks{
			Self:    fmt.Sprintf("/api/v2/notificationEndpoints/%s", edp.GetID()),
			Labels:  fmt.Sprintf("/api/v2/notificationEndpoints/%s/labels", edp.GetID()),
			Members: fmt.Sprintf("/api/v2/notificationEndpoints/%s/members", edp.GetID()),
			Owners:  fmt.Sprintf("/api/v2/notificationEndpoints/%s/owners", edp.GetID()),
		},
		Labels: []influxdb.Label{},
	}

	for _, l := range labels {
		res.Labels = append(res.Labels, *l)
	}

	return res
}

func newNotificationEndpointsResponse(ctx context.Context, edps []influxdb.NotificationEndpoint, labelService influxdb.LabelService, f influxdb.PagingFilter, opts influxdb.FindOptions) *notificationEndpointsResponse {
	resp := &notificationEndpointsResponse{
		NotificationEndpoints: make([]*notificationEndpointResponse, len(edps)),
		Links:                 newPagingLinks(notificationEndpointsPath, opts, f, len(edps)),
	}
	for i, edp := range edps {
		labels, _ := labelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: edp.GetID()})
		resp.NotificationEndpoints[i] = newNotificationEndpointResponse(edp, labels)
	}
	return resp
}

func decodeGetNotificationEndpointRequest(ctx context.Context, r *http.Request) (i influxdb.ID, err error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return i, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	if err := i.DecodeFromString(id); err != nil {
		return i, err
	}
	return i, nil
}

func (h *NotificationEndpointHandler) handleGetNotificationEndpoints(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("notificationEndpoints retrieve request", zap.String("r", fmt.Sprint(r)))
	filter, opts, err := decodeNotificationEndpointFilter(ctx, r)
	if err != nil {
		h.Logger.Debug("failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}
	edps, _, err := h.NotificationEndpointService.FindNotificationEndpoints(ctx, *filter, *opts)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("notificationEndpoints retrieved", zap.String("notificationEndpoints", fmt.Sprint(edps)))

	if err := encodeResponse(ctx, w, http.StatusOK, newNotificationEndpointsResponse(ctx, edps, h.LabelService, filter, *opts)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func (h *NotificationEndpointHandler) handleGetNotificationEndpoint(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("notificationEndpoint retrieve request", zap.String("r", fmt.Sprint(r)))
	id, err := decodeGetNotificationEndpointRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	edp, err := h.NotificationEndpointService.FindNotificationEndpointByID(ctx, id)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("notificationEndpoint retrieved", zap.String("notificationEndpoint", fmt.Sprint(edp)))

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: edp.GetID()})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newNotificationEndpointResponse(edp, labels)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func decodeNotificationEndpointFilter(ctx context.Context, r *http.Request) (*influxdb.NotificationEndpointFilter, *influxdb.FindOptions, error) {
	f := &influxdb.NotificationEndpointFilter{}

	opts, err := decodeFindOptions(ctx, r)
	if err != nil {
		return f, nil, err
	}

	q := r.URL.Query()
	if orgIDStr := q.Get("orgID"); orgIDStr != "" {
		orgID, err := influxdb.IDFromString(orgIDStr)
		if err != nil {
			return f, opts, &influxdb.Error{
				Code: influxdb.EInvalid,
				Msg:  "orgID is invalid",
				Err:  err,
			}
		}
		f.OrgID = orgID
	} else if orgNameStr := q.Get("org"); orgNameStr != "" {
		*f.Org = orgNameStr
	}
	return f, opts, err
}

func decodePostNotificationEndpointRequest(ctx context.Context, r *http.Request) (influxdb.NotificationEndpoint, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	defer r.Body.Close()
	edp, err := endpoint.UnmarshalJSON(buf.Bytes())
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	return edp, nil
}

func decodePutNotificationEndpointRequest(ctx context.Context, r *http.Request) (influxdb.NotificationEndpoint, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	defer r.Body.Close()
	edp, err := endpoint.UnmarshalJSON(buf.Bytes())
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}
	i := new(influxdb.ID)
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	edp.SetID(*i)
	return edp, nil
}

type patchNotificationEndpointRequest struct {
	influxdb.ID
	Update influxdb.NotificationEndpointUpdate
}

func decodePatchNotificationEndpointRequest(ctx context.Context, r *http.Request) (*patchNotificationEndpointRequest, error) {
	req := &patchNotificationEndpointRequest{}
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
	req.ID = i

	upd := &influxdb.NotificationEndpointUpdate{}
	if err := json.NewDecoder(r.Body).Decode(upd); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  err.Error(),
		}
	}
	if err := upd.Valid(); err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  err.Error(),
		}
	}

	req.Update = *upd
	return req, nil
}

// handlePostNotificationEndpoint is the HTTP handler for the POST /api/v2/notificationEndpoints route.
func (h *NotificationEndpointHandler) handlePostNotificationEndpoint(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("notificationEndpoint create request", zap.String("r", fmt.Sprint(r)))
	edp, err := decodePostNotificationEndpointRequest(ctx, r)
	if err != nil {
		h.Logger.Debug("failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.NotificationEndpointService.CreateNotificationEndpoint(ctx, edp, auth.GetUserID()); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("notificationEndpoint created", zap.String("notificationEndpoint", fmt.Sprint(edp)))

	if err := encodeResponse(ctx, w, http.StatusCreated, newNotificationEndpointResponse(edp, []*influxdb.Label{})); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

// handlePutNotificationEndpoint is the HTTP handler for the PUT /api/v2/notificationEndpoints route.
func (h *NotificationEndpointHandler) handlePutNotificationEndpoint(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("notificationEndpoint replace request", zap.String("r", fmt.Sprint(r)))
	edp, err := decodePutNotificationEndpointRequest(ctx, r)
	if err != nil {
		h.Logger.Debug("failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}
	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	edp, err = h.NotificationEndpointService.UpdateNotificationEndpoint(ctx, edp.GetID(), edp, auth.GetUserID())
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: edp.GetID()})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("notificationEndpoint replaced", zap.String("notificationEndpoint", fmt.Sprint(edp)))

	if err := encodeResponse(ctx, w, http.StatusOK, newNotificationEndpointResponse(edp, labels)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

// handlePatchNotificationEndpoint is the HTTP handler for the PATCH /api/v2/notificationEndpoints/:id route.
func (h *NotificationEndpointHandler) handlePatchNotificationEndpoint(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("notificationEndpoint patch request", zap.String("r", fmt.Sprint(r)))
	req, err := decodePatchNotificationEndpointRequest(ctx, r)
	if err != nil {
		h.Logger.Debug("failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	edp, err := h.NotificationEndpointService.PatchNotificationEndpoint(ctx, req.ID, req.Update)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: edp.GetID()})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("notificationEndpoint patch", zap.String("notificationEndpoint", fmt.Sprint(edp)))

	if err := encodeResponse(ctx, w, http.StatusOK, newNotificationEndpointResponse(edp, labels)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func (h *NotificationEndpointHandler) handleDeleteNotificationEndpoint(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("notificationEndpoint delete request", zap.String("r", fmt.Sprint(r)))
	i, err := decodeGetNotificationEndpointRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err = h.NotificationEndpointService.DeleteNotificationEndpoint(ctx, i); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("notificationEndpoint deleted", zap.String("notificationEndpointID", fmt.Sprint(i)))

	w.WriteHeader(http.StatusNoContent)
}
