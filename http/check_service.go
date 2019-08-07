package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/notification/check"
	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"
)

// CheckBackend is all services and associated parameters required to construct
// the CheckBackendHandler.
type CheckBackend struct {
	influxdb.HTTPErrorHandler
	Logger *zap.Logger

	CheckService               influxdb.CheckService
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	UserService                influxdb.UserService
	OrganizationService        influxdb.OrganizationService
}

// NewCheckBackend returns a new instance of CheckBackend.
func NewCheckBackend(b *APIBackend) *CheckBackend {
	return &CheckBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Logger:           b.Logger.With(zap.String("handler", "check")),

		CheckService:               b.CheckService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		OrganizationService:        b.OrganizationService,
	}
}

// CheckHandler is the handler for the check service
type CheckHandler struct {
	*httprouter.Router
	influxdb.HTTPErrorHandler
	Logger *zap.Logger

	CheckService               influxdb.CheckService
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	UserService                influxdb.UserService
	OrganizationService        influxdb.OrganizationService
}

const (
	checksPath            = "/api/v2/checks"
	checksIDPath          = "/api/v2/checks/:id"
	checksIDMembersPath   = "/api/v2/checks/:id/members"
	checksIDMembersIDPath = "/api/v2/checks/:id/members/:userID"
	checksIDOwnersPath    = "/api/v2/checks/:id/owners"
	checksIDOwnersIDPath  = "/api/v2/checks/:id/owners/:userID"
	checksIDLabelsPath    = "/api/v2/checks/:id/labels"
	checksIDLabelsIDPath  = "/api/v2/checks/:id/labels/:lid"
)

// NewCheckHandler returns a new instance of CheckHandler.
func NewCheckHandler(b *CheckBackend) *CheckHandler {
	h := &CheckHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		Logger:           b.Logger,

		CheckService:               b.CheckService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		OrganizationService:        b.OrganizationService,
	}
	h.HandlerFunc("POST", checksPath, h.handlePostCheck)
	h.HandlerFunc("GET", checksPath, h.handleGetChecks)
	h.HandlerFunc("GET", checksIDPath, h.handleGetCheck)
	h.HandlerFunc("DELETE", checksIDPath, h.handleDeleteCheck)
	h.HandlerFunc("PUT", checksIDPath, h.handlePutCheck)
	h.HandlerFunc("PATCH", checksIDPath, h.handlePatchCheck)

	memberBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		Logger:                     b.Logger.With(zap.String("handler", "member")),
		ResourceType:               influxdb.ChecksResourceType,
		UserType:                   influxdb.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", checksIDMembersPath, newPostMemberHandler(memberBackend))
	h.HandlerFunc("GET", checksIDMembersPath, newGetMembersHandler(memberBackend))
	h.HandlerFunc("DELETE", checksIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		Logger:                     b.Logger.With(zap.String("handler", "member")),
		ResourceType:               influxdb.ChecksResourceType,
		UserType:                   influxdb.Owner,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", checksIDOwnersPath, newPostMemberHandler(ownerBackend))
	h.HandlerFunc("GET", checksIDOwnersPath, newGetMembersHandler(ownerBackend))
	h.HandlerFunc("DELETE", checksIDOwnersIDPath, newDeleteMemberHandler(ownerBackend))

	labelBackend := &LabelBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		Logger:           b.Logger.With(zap.String("handler", "label")),
		LabelService:     b.LabelService,
		ResourceType:     influxdb.TelegrafsResourceType,
	}
	h.HandlerFunc("GET", checksIDLabelsIDPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", checksIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", checksIDLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

type checkLinks struct {
	Self    string `json:"self"`
	Labels  string `json:"labels"`
	Members string `json:"members"`
	Owners  string `json:"owners"`
}

type checkResponse struct {
	influxdb.Check
	Labels []influxdb.Label `json:"labels"`
	Links  checkLinks       `json:"links"`
}

func (resp checkResponse) MarshalJSON() ([]byte, error) {
	b1, err := json.Marshal(resp.Check)
	if err != nil {
		return nil, err
	}

	b2, err := json.Marshal(struct {
		Labels []influxdb.Label `json:"labels"`
		Links  checkLinks       `json:"links"`
	}{
		Links:  resp.Links,
		Labels: resp.Labels,
	})
	if err != nil {
		return nil, err
	}

	return []byte(string(b1[:len(b1)-1]) + ", " + string(b2[1:])), nil
}

type checksResponse struct {
	Checks []*checkResponse      `json:"checks"`
	Links  *influxdb.PagingLinks `json:"links"`
}

func newCheckResponse(chk influxdb.Check, labels []*influxdb.Label) *checkResponse {
	// Ensure that we don't expose that this creates a task behind the scene
	chk.ClearPrivateData()

	res := &checkResponse{
		Check: chk,
		Links: checkLinks{
			Self:    fmt.Sprintf("/api/v2/checks/%s", chk.GetID()),
			Labels:  fmt.Sprintf("/api/v2/checks/%s/labels", chk.GetID()),
			Members: fmt.Sprintf("/api/v2/checks/%s/members", chk.GetID()),
			Owners:  fmt.Sprintf("/api/v2/checks/%s/owners", chk.GetID()),
		},
		Labels: []influxdb.Label{},
	}

	for _, l := range labels {
		res.Labels = append(res.Labels, *l)
	}

	return res
}

func newChecksResponse(ctx context.Context, chks []influxdb.Check, labelService influxdb.LabelService, f influxdb.PagingFilter, opts influxdb.FindOptions) *checksResponse {
	resp := &checksResponse{
		Checks: make([]*checkResponse, len(chks)),
		Links:  newPagingLinks(checksPath, opts, f, len(chks)),
	}
	for i, chk := range chks {
		labels, _ := labelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: chk.GetID()})
		resp.Checks[i] = newCheckResponse(chk, labels)
	}
	return resp
}

func decodeGetCheckRequest(ctx context.Context, r *http.Request) (i influxdb.ID, err error) {
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

func (h *CheckHandler) handleGetChecks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("checks retrieve request", zap.String("r", fmt.Sprint(r)))
	filter, opts, err := decodeCheckFilter(ctx, r)
	if err != nil {
		h.Logger.Debug("failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}
	chks, _, err := h.CheckService.FindChecks(ctx, *filter, *opts)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("checks retrieved", zap.String("checks", fmt.Sprint(chks)))

	if err := encodeResponse(ctx, w, http.StatusOK, newChecksResponse(ctx, chks, h.LabelService, filter, *opts)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func (h *CheckHandler) handleGetCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("check retrieve request", zap.String("r", fmt.Sprint(r)))
	id, err := decodeGetCheckRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	chk, err := h.CheckService.FindCheckByID(ctx, id)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("check retrieved", zap.String("check", fmt.Sprint(chk)))

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: chk.GetID()})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newCheckResponse(chk, labels)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func decodeCheckFilter(ctx context.Context, r *http.Request) (*influxdb.CheckFilter, *influxdb.FindOptions, error) {
	f := &influxdb.CheckFilter{}

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

func decodePostCheckRequest(ctx context.Context, r *http.Request) (influxdb.Check, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	defer r.Body.Close()
	chk, err := check.UnmarshalJSON(buf.Bytes())
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	return chk, nil
}

func decodePutCheckRequest(ctx context.Context, r *http.Request) (influxdb.Check, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	defer r.Body.Close()
	chk, err := check.UnmarshalJSON(buf.Bytes())
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
	chk.SetID(*i)
	return chk, nil
}

type patchCheckRequest struct {
	influxdb.ID
	Update influxdb.CheckUpdate
}

func decodePatchCheckRequest(ctx context.Context, r *http.Request) (*patchCheckRequest, error) {
	req := &patchCheckRequest{}
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

	upd := &influxdb.CheckUpdate{}
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

// handlePostCheck is the HTTP handler for the POST /api/v2/checks route.
func (h *CheckHandler) handlePostCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("check create request", zap.String("r", fmt.Sprint(r)))
	chk, err := decodePostCheckRequest(ctx, r)
	if err != nil {
		h.Logger.Debug("failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.CheckService.CreateCheck(ctx, chk); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("check created", zap.String("check", fmt.Sprint(chk)))

	if err := encodeResponse(ctx, w, http.StatusCreated, newCheckResponse(chk, []*influxdb.Label{})); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

// handlePutCheck is the HTTP handler for the PUT /api/v2/checks route.
func (h *CheckHandler) handlePutCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("check replace request", zap.String("r", fmt.Sprint(r)))
	chk, err := decodePutCheckRequest(ctx, r)
	if err != nil {
		h.Logger.Debug("failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	chk, err = h.CheckService.UpdateCheck(ctx, chk.GetID(), chk)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: chk.GetID()})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("check replaced", zap.String("check", fmt.Sprint(chk)))

	if err := encodeResponse(ctx, w, http.StatusOK, newCheckResponse(chk, labels)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

// handlePatchCheck is the HTTP handler for the PATCH /api/v2/checks/:id route.
func (h *CheckHandler) handlePatchCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("check patch request", zap.String("r", fmt.Sprint(r)))
	req, err := decodePatchCheckRequest(ctx, r)
	if err != nil {
		h.Logger.Debug("failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	chk, err := h.CheckService.PatchCheck(ctx, req.ID, req.Update)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: chk.GetID()})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("check patch", zap.String("check", fmt.Sprint(chk)))

	if err := encodeResponse(ctx, w, http.StatusOK, newCheckResponse(chk, labels)); err != nil {
		logEncodingError(h.Logger, r, err)
		return
	}
}

func (h *CheckHandler) handleDeleteCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.Logger.Debug("check delete request", zap.String("r", fmt.Sprint(r)))
	i, err := decodeGetCheckRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err = h.CheckService.DeleteCheck(ctx, i); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.Logger.Debug("check deleted", zap.String("checkID", fmt.Sprint(i)))

	w.WriteHeader(http.StatusNoContent)
}
