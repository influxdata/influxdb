package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"

	"github.com/golang/gddo/httputil"
	"github.com/influxdata/httprouter"
	platform "github.com/influxdata/influxdb"
	pctx "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/telegraf/plugins"
	"go.uber.org/zap"
)

// TelegrafBackend is all services and associated parameters required to construct
// the TelegrafHandler.
type TelegrafBackend struct {
	platform.HTTPErrorHandler
	log *zap.Logger

	TelegrafService            platform.TelegrafConfigStore
	UserResourceMappingService platform.UserResourceMappingService
	LabelService               platform.LabelService
	UserService                platform.UserService
	OrganizationService        platform.OrganizationService
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
	platform.HTTPErrorHandler
	log *zap.Logger

	TelegrafService            platform.TelegrafConfigStore
	UserResourceMappingService platform.UserResourceMappingService
	LabelService               platform.LabelService
	UserService                platform.UserService
	OrganizationService        platform.OrganizationService
}

const (
	telegrafsPath            = "/api/v2/telegrafs"
	telegrafsIDPath          = "/api/v2/telegrafs/:id"
	telegrafsIDMembersPath   = "/api/v2/telegrafs/:id/members"
	telegrafsIDMembersIDPath = "/api/v2/telegrafs/:id/members/:userID"
	telegrafsIDOwnersPath    = "/api/v2/telegrafs/:id/owners"
	telegrafsIDOwnersIDPath  = "/api/v2/telegrafs/:id/owners/:userID"
	telegrafsIDLabelsPath    = "/api/v2/telegrafs/:id/labels"
	telegrafsIDLabelsIDPath  = "/api/v2/telegrafs/:id/labels/:lid"
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
	h.HandlerFunc("POST", telegrafsPath, h.handlePostTelegraf)
	h.HandlerFunc("GET", telegrafsPath, h.handleGetTelegrafs)
	h.HandlerFunc("GET", telegrafsIDPath, h.handleGetTelegraf)
	h.HandlerFunc("DELETE", telegrafsIDPath, h.handleDeleteTelegraf)
	h.HandlerFunc("PUT", telegrafsIDPath, h.handlePutTelegraf)

	memberBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               platform.TelegrafsResourceType,
		UserType:                   platform.Member,
		UserResourceMappingService: b.UserResourceMappingService,
		UserService:                b.UserService,
	}
	h.HandlerFunc("POST", telegrafsIDMembersPath, newPostMemberHandler(memberBackend))
	h.HandlerFunc("GET", telegrafsIDMembersPath, newGetMembersHandler(memberBackend))
	h.HandlerFunc("DELETE", telegrafsIDMembersIDPath, newDeleteMemberHandler(memberBackend))

	ownerBackend := MemberBackend{
		HTTPErrorHandler:           b.HTTPErrorHandler,
		log:                        b.log.With(zap.String("handler", "member")),
		ResourceType:               platform.TelegrafsResourceType,
		UserType:                   platform.Owner,
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
		ResourceType:     platform.TelegrafsResourceType,
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

// MarshalJSON implement the json.Marshaler interface.
// TODO: remove this hack and make labels and links return.
// see: https://github.com/influxdata/influxdb/issues/12457
func (r *telegrafResponse) MarshalJSON() ([]byte, error) {
	// telegrafPluginEncode is the helper struct for json encoding.
	type telegrafPluginEncode struct {
		// Name of the telegraf plugin, exp "docker"
		Name    string         `json:"name"`
		Type    plugins.Type   `json:"type"`
		Comment string         `json:"comment"`
		Config  plugins.Config `json:"config"`
	}

	// telegrafConfigEncode is the helper struct for json encoding.
	type telegrafConfigEncode struct {
		ID          platform.ID                  `json:"id"`
		OrgID       platform.ID                  `json:"orgID,omitempty"`
		Name        string                       `json:"name"`
		Description string                       `json:"description"`
		Agent       platform.TelegrafAgentConfig `json:"agent"`
		Plugins     []telegrafPluginEncode       `json:"plugins"`
		Labels      []platform.Label             `json:"labels"`
		Links       telegrafLinks                `json:"links"`
	}

	tce := new(telegrafConfigEncode)
	*tce = telegrafConfigEncode{
		ID:          r.ID,
		OrgID:       r.OrgID,
		Name:        r.Name,
		Description: r.Description,
		Agent:       r.Agent,
		Plugins:     make([]telegrafPluginEncode, len(r.Plugins)),
		Labels:      r.Labels,
		Links:       r.Links,
	}

	for k, p := range r.Plugins {
		tce.Plugins[k] = telegrafPluginEncode{
			Name:    p.Config.PluginName(),
			Type:    p.Config.Type(),
			Comment: p.Comment,
			Config:  p.Config,
		}
	}

	return json.Marshal(tce)
}

type telegrafResponse struct {
	*platform.TelegrafConfig
	Labels []platform.Label `json:"labels"`
	Links  telegrafLinks    `json:"links"`
}

type telegrafResponses struct {
	TelegrafConfigs []*telegrafResponse `json:"configurations"`
}

func newTelegrafResponse(tc *platform.TelegrafConfig, labels []*platform.Label) *telegrafResponse {
	res := &telegrafResponse{
		TelegrafConfig: tc,
		Links: telegrafLinks{
			Self:    fmt.Sprintf("/api/v2/telegrafs/%s", tc.ID),
			Labels:  fmt.Sprintf("/api/v2/telegrafs/%s/labels", tc.ID),
			Members: fmt.Sprintf("/api/v2/telegrafs/%s/members", tc.ID),
			Owners:  fmt.Sprintf("/api/v2/telegrafs/%s/owners", tc.ID),
		},
		Labels: []platform.Label{},
	}

	for _, l := range labels {
		res.Labels = append(res.Labels, *l)
	}

	return res
}

func newTelegrafResponses(ctx context.Context, tcs []*platform.TelegrafConfig, labelService platform.LabelService) *telegrafResponses {
	resp := &telegrafResponses{
		TelegrafConfigs: make([]*telegrafResponse, len(tcs)),
	}
	for i, c := range tcs {
		labels, _ := labelService.FindResourceLabels(ctx, platform.LabelMappingFilter{ResourceID: c.ID})
		resp.TelegrafConfigs[i] = newTelegrafResponse(c, labels)
	}
	return resp
}

func decodeGetTelegrafRequest(ctx context.Context) (i platform.ID, err error) {
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return i, &platform.Error{
			Code: platform.EInvalid,
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
		w.Write([]byte(tc.TOML()))
	case "application/json":
		labels, err := h.LabelService.FindResourceLabels(ctx, platform.LabelMappingFilter{ResourceID: tc.ID})
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
		w.Write([]byte(tc.TOML()))
	}
}

func decodeTelegrafConfigFilter(ctx context.Context, r *http.Request) (*platform.TelegrafConfigFilter, error) {
	f := &platform.TelegrafConfigFilter{}
	urm, err := decodeUserResourceMappingFilter(ctx, r, platform.TelegrafsResourceType)
	if err == nil {
		f.UserResourceMappingFilter = *urm
	}

	q := r.URL.Query()
	if orgIDStr := q.Get("orgID"); orgIDStr != "" {
		orgID, err := platform.IDFromString(orgIDStr)
		if err != nil {
			return f, &platform.Error{
				Code: platform.EInvalid,
				Msg:  "orgID is invalid",
				Err:  err,
			}
		}
		f.OrgID = orgID
	} else if orgNameStr := q.Get("org"); orgNameStr != "" {
		*f.Organization = orgNameStr
	}
	return f, err
}

func decodePostTelegrafRequest(r *http.Request) (*platform.TelegrafConfig, error) {
	var tc platform.TelegrafConfig
	err := json.NewDecoder(r.Body).Decode(&tc)
	return &tc, err
}

func decodePutTelegrafRequest(ctx context.Context, r *http.Request) (*platform.TelegrafConfig, error) {
	tc := new(platform.TelegrafConfig)
	if err := json.NewDecoder(r.Body).Decode(tc); err != nil {
		return nil, err
	}
	params := httprouter.ParamsFromContext(ctx)
	id := params.ByName("id")
	if id == "" {
		return nil, &platform.Error{
			Code: platform.EInvalid,
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

// handlePostTelegraf is the HTTP handler for the POST /api/v2/telegrafs route.
func (h *TelegrafHandler) handlePostTelegraf(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tc, err := decodePostTelegrafRequest(r)
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

	if err := h.TelegrafService.CreateTelegrafConfig(ctx, tc, auth.GetUserID()); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Telegraf created", zap.String("telegraf", fmt.Sprint(tc)))

	if err := encodeResponse(ctx, w, http.StatusCreated, newTelegrafResponse(tc, []*platform.Label{})); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
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

	labels, err := h.LabelService.FindResourceLabels(ctx, platform.LabelMappingFilter{ResourceID: tc.ID})
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
	client C
	*UserResourceMappingService
}

// NewTelegrafService is a constructor for a telegraf service.
func NewTelegrafService(addr, token string, insecureSkipVerify bool) *TelegrafService {
	return &TelegrafService{
		client: C{
			Addr:               addr,
			Token:              token,
			InsecureSkipVerify: insecureSkipVerify,
		},
		UserResourceMappingService: &UserResourceMappingService{
			Addr:               addr,
			Token:              token,
			InsecureSkipVerify: insecureSkipVerify,
		},
	}
}

var _ platform.TelegrafConfigStore = (*TelegrafService)(nil)

// FindTelegrafConfigByID returns a single telegraf config by ID.
func (s *TelegrafService) FindTelegrafConfigByID(ctx context.Context, id platform.ID) (*platform.TelegrafConfig, error) {
	panic("not implemented")
}

// FindTelegrafConfigs returns a list of telegraf configs that match filter and the total count of matching telegraf configs.
// Additional options provide pagination & sorting.
func (s *TelegrafService) FindTelegrafConfigs(ctx context.Context, filter platform.TelegrafConfigFilter, opt ...platform.FindOptions) ([]*platform.TelegrafConfig, int, error) {
	panic("not implemented")
}

// CreateTelegrafConfig creates a new telegraf config and sets b.ID with the new identifier.
func (s *TelegrafService) CreateTelegrafConfig(ctx context.Context, tc *platform.TelegrafConfig, userID platform.ID) error {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(tc); err != nil {
		return err
	}
	return s.client.post(telegrafsPath, &body).
		RespFn(func(r *http.Response) error {
			var teleResp platform.TelegrafConfig
			if err := json.NewDecoder(r.Body).Decode(&teleResp); err != nil {
				return err
			}
			// sad face >_<
			*tc = teleResp
			return nil
		}).
		Do(ctx)
}

// UpdateTelegrafConfig updates a single telegraf config.
// Returns the new telegraf config after update.
func (s *TelegrafService) UpdateTelegrafConfig(ctx context.Context, id platform.ID, tc *platform.TelegrafConfig, userID platform.ID) (*platform.TelegrafConfig, error) {
	panic("not implemented")
}

// DeleteTelegrafConfig removes a telegraf config by ID.
func (s *TelegrafService) DeleteTelegrafConfig(ctx context.Context, id platform.ID) error {
	return s.client.delete(path.Join(telegrafsPath, id.String())).Do(ctx)
}

// C is a basic http client that can make cReqs with out having to juggle
// the token and so forth. It provides sane defaults for checking response
// statuses, sets auth token when provided, and sets the content type to
// application/json for each request. The token, response checker, and
// content type can be overidden on the cReq as well.
type C struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

func (c *C) delete(urlPath string) *cReq {
	return c.newClientReq(http.MethodDelete, urlPath, nil)
}

func (c *C) post(urlPath string, body io.Reader) *cReq {
	return c.newClientReq(http.MethodPost, urlPath, body)
}

func (c *C) newClientReq(method, urlPath string, body io.Reader) *cReq {
	u, err := NewURL(c.Addr, urlPath)
	if err != nil {
		return &cReq{err: err}
	}

	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return &cReq{err: err}
	}
	if c.Token != "" {
		SetToken(c.Token, req)
	}

	cr := &cReq{
		insecureSkip: c.InsecureSkipVerify,
		req:          req,
		respFn:       CheckError,
	}
	return cr.ContentType("application/json")
}

type cReq struct {
	req          *http.Request
	insecureSkip bool
	respFn       func(*http.Response) error

	err error
}

func (r *cReq) Header(k, v string) *cReq {
	if r.err != nil {
		return r
	}
	r.req.Header.Add(k, v)
	return r
}

func (r *cReq) ContentType(ct string) *cReq {
	return r.Header("Content-Type", ct)
}

func (r *cReq) RespFn(fn func(*http.Response) error) *cReq {
	r.respFn = fn
	return r
}

func (r *cReq) Do(ctx context.Context) error {
	if r.err != nil {
		return r.err
	}
	r.req = r.req.WithContext(ctx)

	resp, err := NewClient(r.req.URL.Scheme, r.insecureSkip).Do(r.req)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body) // drain body completely
		resp.Body.Close()
	}()

	return r.respFn(resp)
}
