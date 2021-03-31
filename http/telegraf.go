package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/golang/gddo/httputil"
	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pctx "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"github.com/influxdata/influxdb/v2/telegraf/plugins"
	"go.uber.org/zap"
)

// TelegrafBackend is all services and associated parameters required to construct
// the TelegrafHandler.
type TelegrafBackend struct {
	errors.HTTPErrorHandler
	log *zap.Logger

	TelegrafService            influxdb.TelegrafConfigStore
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	UserService                influxdb.UserService
	OrganizationService        influxdb.OrganizationService
}

// NewTelegrafBackend returns a new instance of TelegrafBackend.
func NewTelegrafBackend(log *zap.Logger, b *APIBackend) *TelegrafBackend {
	return &TelegrafBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		TelegrafService:            b.TelegrafService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		OrganizationService:        b.OrganizationService,
	}
}

// TelegrafHandler is the handler for the telegraf service
type TelegrafHandler struct {
	*httprouter.Router
	errors.HTTPErrorHandler
	log *zap.Logger

	TelegrafService            influxdb.TelegrafConfigStore
	UserResourceMappingService influxdb.UserResourceMappingService
	LabelService               influxdb.LabelService
	UserService                influxdb.UserService
	OrganizationService        influxdb.OrganizationService
}

const (
	prefixTelegraf           = "/api/v2/telegrafs"
	telegrafsIDPath          = "/api/v2/telegrafs/:id"
	telegrafsIDMembersPath   = "/api/v2/telegrafs/:id/members"
	telegrafsIDMembersIDPath = "/api/v2/telegrafs/:id/members/:userID"
	telegrafsIDOwnersPath    = "/api/v2/telegrafs/:id/owners"
	telegrafsIDOwnersIDPath  = "/api/v2/telegrafs/:id/owners/:userID"
	telegrafsIDLabelsPath    = "/api/v2/telegrafs/:id/labels"
	telegrafsIDLabelsIDPath  = "/api/v2/telegrafs/:id/labels/:lid"

	prefixTelegrafPlugins = "/api/v2/telegraf"
	telegrafPluginsPath   = "/api/v2/telegraf/plugins"
)

// NewTelegrafHandler returns a new instance of TelegrafHandler.
func NewTelegrafHandler(log *zap.Logger, b *TelegrafBackend) *TelegrafHandler {
	h := &TelegrafHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		TelegrafService:            b.TelegrafService,
		UserResourceMappingService: b.UserResourceMappingService,
		LabelService:               b.LabelService,
		UserService:                b.UserService,
		OrganizationService:        b.OrganizationService,
	}
	h.HandlerFunc("POST", prefixTelegraf, h.handlePostTelegraf)
	h.HandlerFunc("GET", prefixTelegraf, h.handleGetTelegrafs)
	h.HandlerFunc("GET", telegrafsIDPath, h.handleGetTelegraf)
	h.HandlerFunc("DELETE", telegrafsIDPath, h.handleDeleteTelegraf)
	h.HandlerFunc("PUT", telegrafsIDPath, h.handlePutTelegraf)

	h.HandlerFunc("GET", telegrafPluginsPath, h.handleGetTelegrafPlugins)

	memberBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               influxdb.TelegrafsResourceType,
		UserType:                   influxdb.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", telegrafsIDMembersPath, newPostMemberHandler(memberBackend))
	h.HandlerFunc("GET", telegrafsIDMembersPath, newGetMembersHandler(memberBackend))
	h.HandlerFunc("DELETE", telegrafsIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               influxdb.TelegrafsResourceType,
		UserType:                   influxdb.Owner,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", telegrafsIDOwnersPath, newPostMemberHandler(ownerBackend))
	h.HandlerFunc("GET", telegrafsIDOwnersPath, newGetMembersHandler(ownerBackend))
	h.HandlerFunc("DELETE", telegrafsIDOwnersIDPath, newDeleteMemberHandler(ownerBackend))

	labelBackend := &LabelBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              b.log.With(zap.String("handler", "label")),
		LabelService:     b.LabelService,
		ResourceType:     influxdb.TelegrafsResourceType,
	}
	h.HandlerFunc("GET", telegrafsIDLabelsPath, newGetLabelsHandler(labelBackend))
	h.HandlerFunc("POST", telegrafsIDLabelsPath, newPostLabelHandler(labelBackend))
	h.HandlerFunc("DELETE", telegrafsIDLabelsIDPath, newDeleteLabelHandler(labelBackend))

	return h
}

type telegrafLinks struct {
	Self    string `json:"self"`
	Labels  string `json:"labels"`
	Members string `json:"members"`
	Owners  string `json:"owners"`
}

type telegrafResponse struct {
	*influxdb.TelegrafConfig
	Labels []influxdb.Label `json:"labels"`
	Links  telegrafLinks    `json:"links"`
}

type telegrafResponses struct {
	TelegrafConfigs []*telegrafResponse `json:"configurations"`
}

func getTelegrafPlugins(t string) (*plugins.TelegrafPlugins, error) {
	if len(t) == 0 {
		return plugins.AvailablePlugins()
	}

	return plugins.ListAvailablePlugins(t)
}

func (h *TelegrafHandler) handleGetTelegrafPlugins(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	telPlugins, err := getTelegrafPlugins(r.URL.Query().Get("type"))
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, telPlugins); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

func newTelegrafResponse(tc *influxdb.TelegrafConfig, labels []*influxdb.Label) *telegrafResponse {
	res := &telegrafResponse{
		TelegrafConfig: tc,
		Links: telegrafLinks{
			Self:    fmt.Sprintf("/api/v2/telegrafs/%s", tc.ID),
			Labels:  fmt.Sprintf("/api/v2/telegrafs/%s/labels", tc.ID),
			Members: fmt.Sprintf("/api/v2/telegrafs/%s/members", tc.ID),
			Owners:  fmt.Sprintf("/api/v2/telegrafs/%s/owners", tc.ID),
		},
		Labels: []influxdb.Label{},
	}

	for _, l := range labels {
		res.Labels = append(res.Labels, *l)
	}

	return res
}

func newTelegrafResponses(ctx context.Context, tcs []*influxdb.TelegrafConfig, labelService influxdb.LabelService) *telegrafResponses {
	resp := &telegrafResponses{
		TelegrafConfigs: make([]*telegrafResponse, len(tcs)),
	}
	for i, c := range tcs {
		labels, _ := labelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: c.ID, ResourceType: influxdb.TelegrafsResourceType})
		resp.TelegrafConfigs[i] = newTelegrafResponse(c, labels)
	}
	return resp
}

func decodeGetTelegrafRequest(ctx context.Context) (i platform.ID, err error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return i, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		}
	}

	if err := i.DecodeFromString(id); err != nil {
		return i, err
	}
	return i, nil
}

func (h *TelegrafHandler) handleGetTelegrafs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	filter, err := decodeTelegrafConfigFilter(ctx, r)
	if err != nil {
		h.log.Debug("Failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}
	tcs, _, err := h.TelegrafService.FindTelegrafConfigs(ctx, *filter)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Telegrafs retrieved", zap.String("telegrafs", fmt.Sprint(tcs)))

	if err := encodeResponse(ctx, w, http.StatusOK, newTelegrafResponses(ctx, tcs, h.LabelService)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

func (h *TelegrafHandler) handleGetTelegraf(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id, err := decodeGetTelegrafRequest(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	tc, err := h.TelegrafService.FindTelegrafConfigByID(ctx, id)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Telegraf retrieved", zap.String("telegraf", fmt.Sprint(tc)))

	offers := []string{"application/toml", "application/json", "application/octet-stream"}
	defaultOffer := "application/toml"
	mimeType := httputil.NegotiateContentType(r, offers, defaultOffer)
	switch mimeType {
	case "application/octet-stream":
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.toml\"", strings.Replace(strings.TrimSpace(tc.Name), " ", "_", -1)))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(tc.Config))
	case "application/json":
		labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: tc.ID, ResourceType: influxdb.TelegrafsResourceType})
		if err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}

		if err := encodeResponse(ctx, w, http.StatusOK, newTelegrafResponse(tc, labels)); err != nil {
			logEncodingError(h.log, r, err)
			return
		}
	case "application/toml":
		w.Header().Set("Content-Type", "application/toml; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(tc.Config))
	}
}

func decodeTelegrafConfigFilter(ctx context.Context, r *http.Request) (*influxdb.TelegrafConfigFilter, error) {
	f := &influxdb.TelegrafConfigFilter{}
	q := r.URL.Query()

	if orgIDStr := q.Get("orgID"); orgIDStr != "" {
		orgID, err := platform.IDFromString(orgIDStr)
		if err != nil {
			return f, &errors.Error{
				Code: errors.EInvalid,
				Msg:  "orgID is invalid",
				Err:  err,
			}
		}
		f.OrgID = orgID
	} else if orgNameStr := q.Get("org"); orgNameStr != "" {
		f.Organization = &orgNameStr
	}

	return f, nil
}

// handlePostTelegraf is the HTTP handler for the POST /api/v2/telegrafs route.
func (h *TelegrafHandler) handlePostTelegraf(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tc := new(influxdb.TelegrafConfig)
	if err := json.NewDecoder(r.Body).Decode(tc); err != nil {
		h.log.Debug("Failed to decode request", zap.Error(err))
		h.HandleHTTPError(ctx, err, w)
		return
	}

	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.TelegrafService.CreateTelegrafConfig(ctx, tc, auth.GetUserID()); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Telegraf created", zap.String("telegraf", fmt.Sprint(tc)))

	if err := encodeResponse(ctx, w, http.StatusCreated, newTelegrafResponse(tc, []*influxdb.Label{})); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

func decodePutTelegrafRequest(ctx context.Context, r *http.Request) (*influxdb.TelegrafConfig, error) {
	tc := new(influxdb.TelegrafConfig)
	if err := json.NewDecoder(r.Body).Decode(tc); err != nil {
		return nil, err
	}
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &errors.Error{
			Code: errors.EInvalid,
			Msg:  "url missing id",
		}
	}
	i := new(platform.ID)
	if err := i.DecodeFromString(id); err != nil {
		return nil, err
	}
	tc.ID = *i
	return tc, nil
}

// handlePutTelegraf is the HTTP handler for the POST /api/v2/telegrafs route.
func (h *TelegrafHandler) handlePutTelegraf(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tc, err := decodePutTelegrafRequest(ctx, r)
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

	tc, err = h.TelegrafService.UpdateTelegrafConfig(ctx, tc.ID, tc, auth.GetUserID())
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	labels, err := h.LabelService.FindResourceLabels(ctx, influxdb.LabelMappingFilter{ResourceID: tc.ID, ResourceType: influxdb.TelegrafsResourceType})
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Telegraf updated", zap.String("telegraf", fmt.Sprint(tc)))

	if err := encodeResponse(ctx, w, http.StatusOK, newTelegrafResponse(tc, labels)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

func (h *TelegrafHandler) handleDeleteTelegraf(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	i, err := decodeGetTelegrafRequest(ctx)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err = h.TelegrafService.DeleteTelegrafConfig(ctx, i); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Telegraf deleted", zap.String("telegrafID", fmt.Sprint(i)))

	w.WriteHeader(http.StatusNoContent)
}

// TelegrafService is an http client that speaks to the telegraf service via HTTP.
type TelegrafService struct {
	client *httpc.Client
	*UserResourceMappingService
}

// NewTelegrafService is a constructor for a telegraf service.
func NewTelegrafService(httpClient *httpc.Client) *TelegrafService {
	return &TelegrafService{
		client: httpClient,
		UserResourceMappingService: &UserResourceMappingService{
			Client: httpClient,
		},
	}
}

var _ influxdb.TelegrafConfigStore = (*TelegrafService)(nil)

// FindTelegrafConfigByID returns a single telegraf config by ID.
func (s *TelegrafService) FindTelegrafConfigByID(ctx context.Context, id platform.ID) (*influxdb.TelegrafConfig, error) {
	var cfg influxdb.TelegrafConfig
	err := s.client.
		Get(prefixTelegraf, id.String()).
		Header("Accept", "application/json").
		DecodeJSON(&cfg).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// FindTelegrafConfigs returns a list of telegraf configs that match filter and the total count of matching telegraf configs.
// Additional options provide pagination & sorting.
func (s *TelegrafService) FindTelegrafConfigs(ctx context.Context, f influxdb.TelegrafConfigFilter, opt ...influxdb.FindOptions) ([]*influxdb.TelegrafConfig, int, error) {
	params := influxdb.FindOptionParams(opt...)
	if f.OrgID != nil {
		params = append(params, [2]string{"orgID", f.OrgID.String()})
	}
	if f.Organization != nil {
		params = append(params, [2]string{"organization", *f.Organization})
	}

	var resp struct {
		Configs []*influxdb.TelegrafConfig `json:"configurations"`
	}
	err := s.client.
		Get(prefixTelegraf).
		QueryParams(params...).
		DecodeJSON(&resp).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	return resp.Configs, len(resp.Configs), nil
}

// CreateTelegrafConfig creates a new telegraf config and sets b.ID with the new identifier.
func (s *TelegrafService) CreateTelegrafConfig(ctx context.Context, tc *influxdb.TelegrafConfig, userID platform.ID) error {
	var teleResp influxdb.TelegrafConfig
	err := s.client.
		PostJSON(tc, prefixTelegraf).
		DecodeJSON(&teleResp).
		Do(ctx)
	if err != nil {
		return err
	}
	*tc = teleResp
	return nil
}

// UpdateTelegrafConfig updates a single telegraf config.
// Returns the new telegraf config after update.
func (s *TelegrafService) UpdateTelegrafConfig(ctx context.Context, id platform.ID, tc *influxdb.TelegrafConfig, userID platform.ID) (*influxdb.TelegrafConfig, error) {
	var teleResp influxdb.TelegrafConfig
	err := s.client.
		PutJSON(tc, prefixTelegraf, id.String()).
		DecodeJSON(&teleResp).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &teleResp, nil
}

// DeleteTelegrafConfig removes a telegraf config by ID.
func (s *TelegrafService) DeleteTelegrafConfig(ctx context.Context, id platform.ID) error {
	return s.client.
		Delete(prefixTelegraf, id.String()).
		Do(ctx)
}
