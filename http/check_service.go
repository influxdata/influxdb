package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"time"

	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pctx "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/notification/check"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"go.uber.org/zap"
)

// CheckBackend is all services and associated parameters required to construct
// the CheckBackendHandler.
type CheckBackend struct {
	influxdb.HTTPErrorHandler
	log *zap.Logger

	AlgoWProxy                 FeatureProxyHandler
	TaskService                influxdb.TaskService
	CheckService               influxdb.CheckService
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	UserService                influxdb.UserService
	OrganizationService        influxdb.OrganizationService
	FluxLanguageService        influxdb.FluxLanguageService
}

// NewCheckBackend returns a new instance of CheckBackend.
func NewCheckBackend(log *zap.Logger, b *APIBackend) *CheckBackend {
	return &CheckBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        log,
		AlgoWProxy:                 b.AlgoWProxy,
		TaskService:                b.TaskService,
		CheckService:               b.CheckService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		OrganizationService:        b.OrganizationService,
		FluxLanguageService:        b.FluxLanguageService,
	}
}

// CheckHandler is the handler for the check service
type CheckHandler struct {
	*httprouter.Router
	influxdb.HTTPErrorHandler
	log *zap.Logger

	TaskService                influxdb.TaskService
	CheckService               influxdb.CheckService
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	UserService                influxdb.UserService
	OrganizationService        influxdb.OrganizationService
	FluxLanguageService        influxdb.FluxLanguageService
}

const (
	prefixChecks          = "/api/v2/checks"
	checksIDPath          = "/api/v2/checks/:id"
	checksIDQueryPath     = "/api/v2/checks/:id/query"
	checksIDMembersPath   = "/api/v2/checks/:id/members"
	checksIDMembersIDPath = "/api/v2/checks/:id/members/:userID"
	checksIDOwnersPath    = "/api/v2/checks/:id/owners"
	checksIDOwnersIDPath  = "/api/v2/checks/:id/owners/:userID"
	checksIDLabelsPath    = "/api/v2/checks/:id/labels"
	checksIDLabelsIDPath  = "/api/v2/checks/:id/labels/:lid"
)

// NewCheckHandler returns a new instance of CheckHandler.
func NewCheckHandler(log *zap.Logger, b *CheckBackend) *CheckHandler {
	h := &CheckHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		CheckService:               b.CheckService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		TaskService:                b.TaskService,
		OrganizationService:        b.OrganizationService,
		FluxLanguageService:        b.FluxLanguageService,
	}

	h.Handler("POST", prefixChecks, withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.handlePostCheck)))
	h.HandlerFunc("GET", prefixChecks, h.handleGetChecks)
	h.HandlerFunc("GET", checksIDPath, h.handleGetCheck)
	h.HandlerFunc("GET", checksIDQueryPath, h.handleGetCheckQuery)
	h.HandlerFunc("DELETE", checksIDPath, h.handleDeleteCheck)
	h.Handler("PUT", checksIDPath, withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.handlePutCheck)))
	h.Handler("PATCH", checksIDPath, withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.handlePatchCheck)))

	memberBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
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
		log:                        b.log.With(zap.String("handler", "member")),
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
		log:              b.log.With(zap.String("handler", "label")),
		LabelService:     b.LabelService,
		ResourceType:     influxdb.ChecksResourceType,
	}
	h.HandlerFunc("GET", checksIDLabelsPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", checksIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", checksIDLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

type checkLinks struct {
	Self    string `json:"self"`
	Labels  string `json:"labels"`
	Members string `json:"members"`
	Owners  string `json:"owners"`
	Query   string `json:"query"`
}

type checkResponse struct {
	influxdb.Check
	Status          string           `json:"status"`
	Labels          []influxdb.Label `json:"labels"`
	Links           checkLinks       `json:"links"`
	LatestCompleted time.Time        `json:"latestCompleted,omitempty"`
	LatestScheduled time.Time        `json:"latestScheduled,omitempty"`
	LastRunStatus   string           `json:"LastRunStatus,omitempty"`
	LastRunError    string           `json:"LastRunError,omitempty"`
}

type postCheckRequest struct {
	influxdb.CheckCreate
	Labels []string `json:"labels"`
}

type decodeLabels struct {
	Labels []string `json:"labels"`
}

func (resp checkResponse) MarshalJSON() ([]byte, error) {
	b1, err := json.Marshal(resp.Check)
	if err != nil {
		return nil, err
	}

	b2, err := json.Marshal(struct {
		Labels          []influxdb.Label `json:"labels"`
		Links           checkLinks       `json:"links"`
		Status          string           `json:"status"`
		LatestCompleted time.Time        `json:"latestCompleted,omitempty"`
		LatestScheduled time.Time        `json:"latestScheduled,omitempty"`
		LastRunStatus   string           `json:"lastRunStatus,omitempty"`
		LastRunError    string           `json:"lastRunError,omitempty"`
	}{
		Links:           resp.Links,
		Labels:          resp.Labels,
		Status:          resp.Status,
		LatestCompleted: resp.LatestCompleted,
		LatestScheduled: resp.LatestScheduled,
		LastRunStatus:   resp.LastRunStatus,
		LastRunError:    resp.LastRunError,
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

func (h *CheckHandler) newCheckResponse(ctx context.Context, chk influxdb.Check, labels []*influxdb.Label) (*checkResponse, error) {
	// TODO(desa): this should be handled in the check and not exposed in http land, but is currently blocking the FE. https://github.com/influxdata/influxdb/issues/15259
	task, err := h.TaskService.FindTaskByID(ctx, chk.GetTaskID())
	if err != nil {
		return nil, err
	}

	// Ensure that we don't expose that this creates a task behind the scene
	chk.ClearPrivateData()

	res := &checkResponse{
		Check: chk,
		Links: checkLinks{
			Self:    fmt.Sprintf("/api/v2/checks/%s", chk.GetID()),
			Labels:  fmt.Sprintf("/api/v2/checks/%s/labels", chk.GetID()),
			Members: fmt.Sprintf("/api/v2/checks/%s/members", chk.GetID()),
			Owners:  fmt.Sprintf("/api/v2/checks/%s/owners", chk.GetID()),
			Query:   fmt.Sprintf("/api/v2/checks/%s/query", chk.GetID()),
		},
		Labels:          []influxdb.Label{},
		LatestCompleted: task.LatestCompleted,
		LatestScheduled: task.LatestScheduled,
		LastRunStatus:   task.LastRunStatus,
		LastRunError:    task.LastRunError,
	}

	for _, l := range labels {
		res.Labels = append(res.Labels, *l)
	}

	res.Status = task.Status

	return res, nil
}

func (h *CheckHandler) newChecksResponse(ctx context.Context, chks []influxdb.Check, labelService influxdb.LabelService, f influxdb.PagingFilter, opts influxdb.FindOptions) *checksResponse {
	resp := &checksResponse{
		Checks: []*checkResponse{},
		Links:  influxdb.NewPagingLinks(prefixChecks, opts, f, len(chks)),
	}
	for _, chk := range chks {
		labels, _ := labelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: chk.GetID(), ResourceType: influxdb.ChecksResourceType})
		cr, err := h.newCheckResponse(ctx, chk, labels)
		if err != nil {
			h.log.Info("Failed to retrieve task associated with check", zap.String("checkID", chk.GetID().String()))
			continue
		}

		resp.Checks = append(resp.Checks, cr)
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
	filter, opts, err := decodeCheckFilter(ctx, r)
	if err != nil {
		h.log.Debug("Failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}
	chks, _, err := h.CheckService.FindChecks(ctx, *filter, *opts)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Checks retrieved", zap.String("checks", fmt.Sprint(chks)))

	if err := encodeResponse(ctx, w, http.StatusOK, h.newChecksResponse(ctx, chks, h.LabelService, filter, *opts)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

func (h *CheckHandler) handleGetCheckQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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
	flux, err := chk.GenerateFlux(h.FluxLanguageService)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Check query retrieved", zap.String("check query", flux))
	if err := encodeResponse(ctx, w, http.StatusOK, newFluxResponse(flux)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type fluxResp struct {
	Flux string `json:"flux"`
}

func newFluxResponse(flux string) fluxResp {
	return fluxResp{
		Flux: flux,
	}
}

func (h *CheckHandler) handleGetCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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
	h.log.Debug("Check retrieved", zap.String("check", fmt.Sprint(chk)))

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: chk.GetID(), ResourceType: influxdb.ChecksResourceType})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	cr, err := h.newCheckResponse(ctx, chk, labels)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, cr); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

func decodeCheckFilter(ctx context.Context, r *http.Request) (*influxdb.CheckFilter, *influxdb.FindOptions, error) {
	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		return nil, nil, err
	}
	f := &influxdb.CheckFilter{
		UserResourceMappingFilter: influxdb.UserResourceMappingFilter{
			UserID:       auth.GetUserID(),
			ResourceType: influxdb.ChecksResourceType,
		},
	}

	opts, err := influxdb.DecodeFindOptions(r)
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
		f.Org = &orgNameStr
	}
	return f, opts, err
}

type decodeStatus struct {
	Status influxdb.Status `json:"status"`
}

func decodePostCheckRequest(r *http.Request) (postCheckRequest, error) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return postCheckRequest{}, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	defer r.Body.Close()

	chk, err := check.UnmarshalJSON(b)
	if err != nil {
		return postCheckRequest{}, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	var ds decodeStatus
	if err := json.Unmarshal(b, &ds); err != nil {
		return postCheckRequest{}, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	var dl decodeLabels
	if err := json.Unmarshal(b, &dl); err != nil {
		return postCheckRequest{}, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	return postCheckRequest{
		CheckCreate: influxdb.CheckCreate{
			Check:  chk,
			Status: ds.Status,
		},
		Labels: dl.Labels,
	}, nil
}

func decodePutCheckRequest(ctx context.Context, lang influxdb.FluxLanguageService, r *http.Request) (influxdb.CheckCreate, error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return influxdb.CheckCreate{}, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "url missing id",
		}
	}

	i := new(influxdb.ID)
	if err := i.DecodeFromString(id); err != nil {
		return influxdb.CheckCreate{}, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid check id format",
		}
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return influxdb.CheckCreate{}, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "unable to read HTTP body",
			Err:  err,
		}
	}
	defer r.Body.Close()

	chk, err := check.UnmarshalJSON(b)
	if err != nil {
		return influxdb.CheckCreate{}, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "malformed check body",
			Err:  err,
		}
	}
	chk.SetID(*i)

	if err := chk.Valid(lang); err != nil {
		return influxdb.CheckCreate{}, err
	}

	var ds decodeStatus
	err = json.Unmarshal(b, &ds)
	if err != nil {
		return influxdb.CheckCreate{}, &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}

	return influxdb.CheckCreate{
		Check:  chk,
		Status: ds.Status,
	}, nil
}

type patchCheckRequest struct {
	influxdb.ID
	Update influxdb.CheckUpdate
}

func decodePatchCheckRequest(ctx context.Context, r *http.Request) (*patchCheckRequest, error) {
	id := httprouter.ParamsFromContext(ctx).ByName("id")
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

	var upd influxdb.CheckUpdate
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
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

	return &patchCheckRequest{
		ID:     i,
		Update: upd,
	}, nil
}

// handlePostCheck is the HTTP handler for the POST /api/v2/checks route.
func (h *CheckHandler) handlePostCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	chk, err := decodePostCheckRequest(r)
	if err != nil {
		h.log.Debug("Failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.CheckService.CreateCheck(ctx, chk.CheckCreate, auth.GetUserID()); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels := h.mapNewCheckLabels(ctx, chk.CheckCreate, chk.Labels)

	cr, err := h.newCheckResponse(ctx, chk, labels)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, cr); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// mapNewCheckLabels takes label ids from create check and maps them to the newly created check
func (h *CheckHandler) mapNewCheckLabels(ctx context.Context, chk influxdb.CheckCreate, labels []string) []*influxdb.Label {
	var ls []*influxdb.Label
	for _, sid := range labels {
		var lid influxdb.ID
		err := lid.DecodeFromString(sid)

		if err != nil {
			continue
		}

		label, err := h.LabelService.FindLabelByID(ctx, lid)
		if err != nil {
			continue
		}

		mapping := influxdb.LabelMapping{
			LabelID:      label.ID,
			ResourceID:   chk.GetID(),
			ResourceType: influxdb.ChecksResourceType,
		}

		err = h.LabelService.CreateLabelMapping(ctx, &mapping)
		if err != nil {
			continue
		}

		ls = append(ls, label)
	}
	return ls
}

// handlePutCheck is the HTTP handler for the PUT /api/v2/checks route.
func (h *CheckHandler) handlePutCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chk, err := decodePutCheckRequest(ctx, h.FluxLanguageService, r)
	if err != nil {
		h.log.Debug("Failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	c, err := h.CheckService.UpdateCheck(ctx, chk.GetID(), chk)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: c.GetID(), ResourceType: influxdb.ChecksResourceType})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Check replaced", zap.String("check", fmt.Sprint(c)))

	cr, err := h.newCheckResponse(ctx, c, labels)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, cr); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// handlePatchCheck is the HTTP handler for the PATCH /api/v2/checks/:id route.
func (h *CheckHandler) handlePatchCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePatchCheckRequest(ctx, r)
	if err != nil {
		h.log.Debug("Failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	chk, err := h.CheckService.PatchCheck(ctx, req.ID, req.Update)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: chk.GetID(), ResourceType: influxdb.ChecksResourceType})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Check patch", zap.String("check", fmt.Sprint(chk)))

	cr, err := h.newCheckResponse(ctx, chk, labels)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, cr); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

func (h *CheckHandler) handleDeleteCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	i, err := decodeGetCheckRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err = h.CheckService.DeleteCheck(ctx, i); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Check deleted", zap.String("checkID", fmt.Sprint(i)))

	w.WriteHeader(http.StatusNoContent)
}

func checkIDPath(id influxdb.ID) string {
	return path.Join(prefixChecks, id.String())
}

// CheckService is a client to interact with the handlers in this package over HTTP.
// It does not implement influxdb.CheckService because it returns a concrete representation of the API response
// and influxdb.Check as returned by that interface is not appropriate for this use case.
type CheckService struct {
	Client *httpc.Client
}

// FindCheckByID returns the Check matching the ID.
func (s *CheckService) FindCheckByID(ctx context.Context, id influxdb.ID) (*Check, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var cr Check
	err := s.Client.
		Get(checkIDPath(id)).
		DecodeJSON(&cr).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return &cr, nil
}

// FindCheck returns the first check matching the filter.
func (s *CheckService) FindCheck(ctx context.Context, filter influxdb.CheckFilter) (*Check, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	cs, n, err := s.FindChecks(ctx, filter)
	if err != nil {
		return nil, err
	}

	if n == 0 && filter.Name != nil {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Op:   influxdb.OpFindBucket,
			Msg:  fmt.Sprintf("check %q not found", *filter.Name),
		}
	} else if n == 0 {
		return nil, &influxdb.Error{
			Code: influxdb.ENotFound,
			Op:   influxdb.OpFindBucket,
			Msg:  "check not found",
		}
	}

	return cs[0], nil
}

// FindChecks returns a list of checks that match filter and the total count of matching checks.
// Additional options provide pagination & sorting.
func (s *CheckService) FindChecks(ctx context.Context, filter influxdb.CheckFilter, opt ...influxdb.FindOptions) ([]*Check, int, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	params := influxdb.FindOptionParams(opt...)
	if filter.OrgID != nil {
		params = append(params, [2]string{"orgID", filter.OrgID.String()})
	}
	if filter.Org != nil {
		params = append(params, [2]string{"org", *filter.Org})
	}
	if filter.ID != nil {
		params = append(params, [2]string{"id", filter.ID.String()})
	}
	if filter.Name != nil {
		params = append(params, [2]string{"name", *filter.Name})
	}

	var cr Checks
	err := s.Client.
		Get(prefixChecks).
		QueryParams(params...).
		DecodeJSON(&cr).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	return cr.Checks, len(cr.Checks), nil
}

// CreateCheck creates a new check.
func (s *CheckService) CreateCheck(ctx context.Context, c *Check) (*Check, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var r Check
	err := s.Client.
		PostJSON(c, prefixChecks).
		DecodeJSON(&r).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

// UpdateCheck updates a check.
func (s *CheckService) UpdateCheck(ctx context.Context, id influxdb.ID, u *Check) (*Check, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var r Check
	err := s.Client.
		PutJSON(u, checkIDPath(id)).
		DecodeJSON(&r).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

// PatchCheck changes the status, description or name of a check.
func (s *CheckService) PatchCheck(ctx context.Context, id influxdb.ID, u influxdb.CheckUpdate) (*Check, error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var r Check
	err := s.Client.
		PutJSON(u, checkIDPath(id)).
		DecodeJSON(&r).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	return &r, nil
}

// DeleteCheck removes a check.
func (s *CheckService) DeleteCheck(ctx context.Context, id influxdb.ID) error {
	return s.Client.
		Delete(checkIDPath(id)).
		Do(ctx)
}

// TODO(gavincabbage): These structures should be in a common place, like other models,
// 		but the common influxdb.Check is an interface that is not appropriate for an API client.
type Checks struct {
	Checks []*Check              `json:"checks"`
	Links  *influxdb.PagingLinks `json:"links"`
}

type Check struct {
	ID                    influxdb.ID       `json:"id,omitempty"`
	Name                  string            `json:"name"`
	OrgID                 influxdb.ID       `json:"orgID,omitempty"`
	OwnerID               influxdb.ID       `json:"ownerID,omitempty"`
	CreatedAt             time.Time         `json:"createdAt,omitempty"`
	UpdatedAt             time.Time         `json:"updatedAt,omitempty"`
	Query                 *CheckQuery       `json:"query"`
	Status                influxdb.Status   `json:"status"`
	Description           string            `json:"description"`
	LatestCompleted       time.Time         `json:"latestCompleted"`
	LastRunStatus         string            `json:"lastRunStatus"`
	LastRunError          string            `json:"lastRunError"`
	Labels                []*influxdb.Label `json:"labels"`
	Links                 *CheckLinks       `json:"links"`
	Type                  string            `json:"type"`
	TimeSince             string            `json:"timeSince"`
	StaleTime             string            `json:"staleTime"`
	ReportZero            bool              `json:"reportZero"`
	Level                 string            `json:"level"`
	Every                 string            `json:"every"`
	Offset                string            `json:"offset"`
	Tags                  []*influxdb.Tag   `json:"tags"`
	StatusMessageTemplate string            `json:"statusMessageTemplate"`
	Thresholds            []*CheckThreshold `json:"thresholds"`
}

type CheckQuery struct {
	Text          string              `json:"text"`
	EditMode      string              `json:"editMode"`
	Name          string              `json:"name"`
	BuilderConfig *CheckBuilderConfig `json:"builderConfig"`
}

type CheckBuilderConfig struct {
	Buckets []string `json:"buckets"`
	Tags    []struct {
		Key                   string   `json:"key"`
		Values                []string `json:"values"`
		AggregateFunctionType string   `json:"aggregateFunctionType"`
	} `json:"tags"`
	Functions []struct {
		Name string `json:"name"`
	} `json:"functions"`
	AggregateWindow struct {
		Period string `json:"period"`
	} `json:"aggregateWindow"`
}

type CheckLinks struct {
	Self    string `json:"self"`
	Labels  string `json:"labels"`
	Members string `json:"members"`
	Owners  string `json:"owners"`
	Query   string `json:"query"`
}

type CheckThreshold struct {
	check.ThresholdConfigBase
	Type   string  `json:"type"`
	Value  float64 `json:"value,omitempty"`
	Min    float64 `json:"min,omitempty"`
	Max    float64 `json:"max,omitempty"`
	Within bool    `json:"within"`
}
