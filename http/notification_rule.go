package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/influxdb"
	pctx "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/notification/rule"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// NotificationRuleBackend is all services and associated parameters required to construct
// the NotificationRuleBackendHandler.
type NotificationRuleBackend struct {
	influxdb.HTTPErrorHandler
	Logger *zap.Logger

	NotificationRuleStore      influxdb.NotificationRuleStore
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	UserService                influxdb.UserService
	OrganizationService        influxdb.OrganizationService
}

// NewNotificationRuleBackend returns a new instance of NotificationRuleBackend.
func NewNotificationRuleBackend(b *APIBackend) *NotificationRuleBackend {
	return &NotificationRuleBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Logger:           b.Logger.With(zap.String("handler", "notification_rule")),

		NotificationRuleStore:      b.NotificationRuleStore,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		OrganizationService:        b.OrganizationService,
	}
}

// NotificationRuleHandler is the handler for the notification rule service
type NotificationRuleHandler struct {
	*httprouter.Router
	influxdb.HTTPErrorHandler
	Logger *zap.Logger

	NotificationRuleStore      influxdb.NotificationRuleStore
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	UserService                influxdb.UserService
	OrganizationService        influxdb.OrganizationService
}

const (
	notificationRulesPath            = "/api/v2/notificationRules"
	notificationRulesIDPath          = "/api/v2/notificationRules/:id"
	notificationRulesIDMembersPath   = "/api/v2/notificationRules/:id/members"
	notificationRulesIDMembersIDPath = "/api/v2/notificationRules/:id/members/:userID"
	notificationRulesIDOwnersPath    = "/api/v2/notificationRules/:id/owners"
	notificationRulesIDOwnersIDPath  = "/api/v2/notificationRules/:id/owners/:userID"
	notificationRulesIDLabelsPath    = "/api/v2/notificationRules/:id/labels"
	notificationRulesIDLabelsIDPath  = "/api/v2/notificationRules/:id/labels/:lid"
)

// NewNotificationRuleHandler returns a new instance of NotificationRuleHandler.
func NewNotificationRuleHandler(b *NotificationRuleBackend) *NotificationRuleHandler {
	h := &NotificationRuleHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		Logger:           b.Logger,

		NotificationRuleStore:      b.NotificationRuleStore,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		OrganizationService:        b.OrganizationService,
	}
	h.HandlerFunc("POST", notificationRulesPath, h.handlePostNotificationRule)
	h.HandlerFunc("GET", notificationRulesPath, h.handleGetNotificationRules)
	h.HandlerFunc("GET", notificationRulesIDPath, h.handleGetNotificationRule)
	h.HandlerFunc("DELETE", notificationRulesIDPath, h.handleDeleteNotificationRule)
	h.HandlerFunc("PUT", notificationRulesIDPath, h.handlePutNotificationRule)
	h.HandlerFunc("PATCH", notificationRulesIDPath, h.handlePatchNotificationRule)

	memberBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		Logger:                     b.Logger.With(zap.String("handler", "member")),
		ResourceType:               influxdb.NotificationRuleResourceType,
		UserType:                   influxdb.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", notificationRulesIDMembersPath, newPostMemberHandler(memberBackend))
	h.HandlerFunc("GET", notificationRulesIDMembersPath, newGetMembersHandler(memberBackend))
	h.HandlerFunc("DELETE", notificationRulesIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		Logger:                     b.Logger.With(zap.String("handler", "member")),
		ResourceType:               influxdb.NotificationRuleResourceType,
		UserType:                   influxdb.Owner,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", notificationRulesIDOwnersPath, newPostMemberHandler(ownerBackend))
	h.HandlerFunc("GET", notificationRulesIDOwnersPath, newGetMembersHandler(ownerBackend))
	h.HandlerFunc("DELETE", notificationRulesIDOwnersIDPath, newDeleteMemberHandler(ownerBackend))

	labelBackend := &LabelBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Logger:           b.Logger.With(zap.String("handler", "label")),
		LabelService:     b.LabelService,
		ResourceType:     influxdb.TelegrafsResourceType,
	}
	h.HandlerFunc("GET", notificationRulesIDLabelsIDPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", notificationRulesIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", notificationRulesIDLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

type notificationRuleLinks struct {
	Self    string `json:"self"`
	Labels  string `json:"labels"`
	Members string `json:"members"`
	Owners  string `json:"owners"`
}

type notificationRuleResponse struct {
	influxdb.NotificationRule
	Labels []influxdb.Label      `json:"labels"`
	Links  notificationRuleLinks `json:"links"`
}

func (resp notificationRuleResponse) MarshalJSON() ([]byte, error) {
	b1, err := json.Marshal(resp.NotificationRule)
	if err != nil {
		return nil, err
	}

	b2, err := json.Marshal(struct {
		Labels []influxdb.Label      `json:"labels"`
		Links  notificationRuleLinks `json:"links"`
	}{
		Links:  resp.Links,
		Labels: resp.Labels,
	})
	if err != nil {
		return nil, err
	}

	return []byte(string(b1[:len(b1)-1]) + ", " + string(b2[1:])), nil
}

type notificationRulesResponse struct {
	NotificationRules []*notificationRuleResponse `json:"notificationRules"`
	Links             *influxdb.PagingLinks       `json:"links"`
}

func newNotificationRuleResponse(nr influxdb.NotificationRule, labels []*influxdb.Label) *notificationRuleResponse {
	res := &notificationRuleResponse{
		NotificationRule: nr,
		Links: notificationRuleLinks{
			Self:    fmt.Sprintf("/api/v2/notificationRules/%s", nr.GetID()),
			Labels:  fmt.Sprintf("/api/v2/notificationRules/%s/labels", nr.GetID()),
			Members: fmt.Sprintf("/api/v2/notificationRules/%s/members", nr.GetID()),
			Owners:  fmt.Sprintf("/api/v2/notificationRules/%s/owners", nr.GetID()),
		},
		Labels: []influxdb.Label{},
	}

	for _, l := range labels {
		res.Labels = append(res.Labels, *l)
	}

	return res
}

func newNotificationRulesResponse(ctx context.Context, nrs []influxdb.NotificationRule, labelService influxdb.LabelService, f influxdb.PagingFilter, opts influxdb.FindOptions) *notificationRulesResponse {
	resp := &notificationRulesResponse{
		NotificationRules: make([]*notificationRuleResponse, len(nrs)),
		Links:             newPagingLinks(notificationRulesPath, opts, f, len(nrs)),
	}
	for i, nr := range nrs {
		labels, _ := labelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: nr.GetID()})
		resp.NotificationRules[i] = newNotificationRuleResponse(nr, labels)
	}
	return resp
}

func decodeGetNotificationRuleRequest(ctx context.Context, r *http.Request) (i influxdb.ID, err error) {
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

func (h *NotificationRuleHandler) handleGetNotificationRules(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("notification rules retrieve request", zap.String("r", fmt.Sprint(r)))
	filter, opts, err := decodeNotificationRuleFilter(ctx, r)
	if err != nil {
		h.Logger.Debug("failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}
	nrs, _, err := h.NotificationRuleStore.FindNotificationRules(ctx, *filter, *opts)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("notification rules retrieved", zap.String("notificationRules", fmt.Sprint(nrs)))

	if err := encodeResponse(ctx, w, http.StatusOK, newNotificationRulesResponse(ctx, nrs, h.LabelService, filter, *opts)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func (h *NotificationRuleHandler) handleGetNotificationRule(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("notification rule retrieve request", zap.String("r", fmt.Sprint(r)))
	id, err := decodeGetNotificationRuleRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	nr, err := h.NotificationRuleStore.FindNotificationRuleByID(ctx, id)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("notification rule retrieved", zap.String("notificationRule", fmt.Sprint(nr)))

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: nr.GetID()})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newNotificationRuleResponse(nr, labels)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func decodeNotificationRuleFilter(ctx context.Context, r *http.Request) (*influxdb.NotificationRuleFilter, *influxdb.FindOptions, error) {
	f := &influxdb.NotificationRuleFilter{}
	urm, err := decodeUserResourceMappingFilter(ctx, r, influxdb.NotificationRuleResourceType)
	if err == nil {
		f.UserResourceMappingFilter = *urm
	}

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
		*f.Organization = orgNameStr
	}
	return f, opts, err
}

func decodeUserResourceMappingFilter(ctx context.Context, r *http.Request, typ influxdb.ResourceType) (*influxdb.UserResourceMappingFilter, error) {
	q := r.URL.Query()
	f := &influxdb.UserResourceMappingFilter{
		ResourceType: typ,
	}
	if idStr := q.Get("resourceID"); idStr != "" {
		id, err := influxdb.IDFromString(idStr)
		if err != nil {
			return nil, err
		}
		f.ResourceID = *id
	}

	if idStr := q.Get("userID"); idStr != "" {
		id, err := influxdb.IDFromString(idStr)
		if err != nil {
			return nil, err
		}
		f.UserID = *id
	}
	return f, nil
}

func decodePostNotificationRuleRequest(ctx context.Context, r *http.Request) (influxdb.NotificationRule, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	defer r.Body.Close()
	nr, err := rule.UnmarshalJSON(buf.Bytes())
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	return nr, nil
}

func decodePutNotificationRuleRequest(ctx context.Context, r *http.Request) (influxdb.NotificationRule, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	defer r.Body.Close()
	nr, err := rule.UnmarshalJSON(buf.Bytes())
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
	nr.SetID(*i)
	return nr, nil
}

type patchNotificationRuleRequest struct {
	influxdb.ID
	Update influxdb.NotificationRuleUpdate
}

func decodePatchNotificationRuleRequest(ctx context.Context, r *http.Request) (*patchNotificationRuleRequest, error) {
	req := &patchNotificationRuleRequest{}
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

	upd := &influxdb.NotificationRuleUpdate{}
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

// handlePostNotificationRule is the HTTP handler for the POST /api/v2/notificationRules route.
func (h *NotificationRuleHandler) handlePostNotificationRule(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("notification rule create request", zap.String("r", fmt.Sprint(r)))
	nr, err := decodePostNotificationRuleRequest(ctx, r)
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

	if err := h.NotificationRuleStore.CreateNotificationRule(ctx, nr, auth.GetUserID()); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("notification rule created", zap.String("notificationRule", fmt.Sprint(nr)))

	if err := encodeResponse(ctx, w, http.StatusCreated, newNotificationRuleResponse(nr, []*influxdb.Label{})); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

// handlePutNotificationRule is the HTTP handler for the PUT /api/v2/notificationRule route.
func (h *NotificationRuleHandler) handlePutNotificationRule(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("notification rule update request", zap.String("r", fmt.Sprint(r)))
	nr, err := decodePutNotificationRuleRequest(ctx, r)
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

	nr, err = h.NotificationRuleStore.UpdateNotificationRule(ctx, nr.GetID(), nr, auth.GetUserID())
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: nr.GetID()})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("notification rule updated", zap.String("notificationRule", fmt.Sprint(nr)))

	if err := encodeResponse(ctx, w, http.StatusOK, newNotificationRuleResponse(nr, labels)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

// handlePatchNotificationRule is the HTTP handler for the PATCH /api/v2/notificationRule/:id route.
func (h *NotificationRuleHandler) handlePatchNotificationRule(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("notification rule patch request", zap.String("r", fmt.Sprint(r)))
	req, err := decodePatchNotificationRuleRequest(ctx, r)
	if err != nil {
		h.Logger.Debug("failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	nr, err := h.NotificationRuleStore.PatchNotificationRule(ctx, req.ID, req.Update)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: nr.GetID()})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("notification rule patch", zap.String("notificationRule", fmt.Sprint(nr)))

	if err := encodeResponse(ctx, w, http.StatusOK, newNotificationRuleResponse(nr, labels)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func (h *NotificationRuleHandler) handleDeleteNotificationRule(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("notification rule delete request", zap.String("r", fmt.Sprint(r)))
	i, err := decodeGetNotificationRuleRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err = h.NotificationRuleStore.DeleteNotificationRule(ctx, i); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("notification rule deleted", zap.String("notificationRuleID", fmt.Sprint(i)))

	w.WriteHeader(http.StatusNoContent)
}
