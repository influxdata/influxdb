package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/flux/repl"
	"github.com/influxdata/httprouter"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/influxql"
	"go.uber.org/zap"
)

const (
	prefixSources = "/api/v2/sources"
)

type sourceResponse struct {
	*platform.Source
	Links map[string]interface{} `json:"links"`
}

func newSourceResponse(s *platform.Source) *sourceResponse {
	s.Password = ""
	s.SharedSecret = ""

	if s.Type == platform.SelfSourceType {
		return &sourceResponse{
			Source: s,
			Links: map[string]interface{}{
				"self":    fmt.Sprintf("%s/%s", prefixSources, s.ID.String()),
				"query":   fmt.Sprintf("%s/%s/query", prefixSources, s.ID.String()),
				"buckets": fmt.Sprintf("%s/%s/buckets", prefixSources, s.ID.String()),
				"health":  fmt.Sprintf("%s/%s/health", prefixSources, s.ID.String()),
			},
		}
	}

	return &sourceResponse{
		Source: s,
		Links: map[string]interface{}{
			"self":    fmt.Sprintf("%s/%s", prefixSources, s.ID.String()),
			"query":   fmt.Sprintf("%s/%s/query", prefixSources, s.ID.String()),
			"buckets": fmt.Sprintf("%s/%s/buckets", prefixSources, s.ID.String()),
			"health":  fmt.Sprintf("%s/%s/health", prefixSources, s.ID.String()),
		},
	}
}

type sourcesResponse struct {
	Sources []*sourceResponse      `json:"sources"`
	Links   map[string]interface{} `json:"links"`
}

func newSourcesResponse(srcs []*platform.Source) *sourcesResponse {
	res := &sourcesResponse{
		Links: map[string]interface{}{
			"self": prefixSources,
		},
	}

	res.Sources = make([]*sourceResponse, 0, len(srcs))
	for _, src := range srcs {
		res.Sources = append(res.Sources, newSourceResponse(src))
	}

	return res
}

// SourceBackend is all services and associated parameters required to construct
// the SourceHandler.
type SourceBackend struct {
	platform.HTTPErrorHandler
	log *zap.Logger

	SourceService   platform.SourceService
	LabelService    platform.LabelService
	BucketService   platform.BucketService
	NewQueryService func(s *platform.Source) (query.ProxyQueryService, error)
}

// NewSourceBackend returns a new instance of SourceBackend.
func NewSourceBackend(log *zap.Logger, b *APIBackend) *SourceBackend {
	return &SourceBackend{
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		SourceService:   b.SourceService,
		LabelService:    b.LabelService,
		BucketService:   b.BucketService,
		NewQueryService: b.NewQueryService,
	}
}

// SourceHandler is a handler for sources
type SourceHandler struct {
	*httprouter.Router
	platform.HTTPErrorHandler
	log           *zap.Logger
	SourceService platform.SourceService
	LabelService  platform.LabelService
	BucketService platform.BucketService

	// TODO(desa): this was done so in order to remove an import cycle and to allow
	// for http mocking.
	NewQueryService func(s *platform.Source) (query.ProxyQueryService, error)
}

// NewSourceHandler returns a new instance of SourceHandler.
func NewSourceHandler(log *zap.Logger, b *SourceBackend) *SourceHandler {
	h := &SourceHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		SourceService:   b.SourceService,
		LabelService:    b.LabelService,
		BucketService:   b.BucketService,
		NewQueryService: b.NewQueryService,
	}

	h.HandlerFunc("POST", prefixSources, h.handlePostSource)
	h.HandlerFunc("GET", "/api/v2/sources", h.handleGetSources)
	h.HandlerFunc("GET", "/api/v2/sources/:id", h.handleGetSource)
	h.HandlerFunc("PATCH", "/api/v2/sources/:id", h.handlePatchSource)
	h.HandlerFunc("DELETE", "/api/v2/sources/:id", h.handleDeleteSource)

	h.HandlerFunc("GET", "/api/v2/sources/:id/buckets", h.handleGetSourcesBuckets)
	h.HandlerFunc("POST", "/api/v2/sources/:id/query", h.handlePostSourceQuery)
	h.HandlerFunc("GET", "/api/v2/sources/:id/health", h.handleGetSourceHealth)

	return h
}

func decodeSourceQueryRequest(r *http.Request) (*query.ProxyRequest, error) {
	// starts here
	request := struct {
		Spec           *flux.Spec  `json:"spec"`
		Query          string      `json:"query"`
		Type           string      `json:"type"`
		DB             string      `json:"db"`
		RP             string      `json:"rp"`
		Cluster        string      `json:"cluster"`
		OrganizationID platform.ID `json:"organizationID"`
		// TODO(desa): support influxql dialect
		Dialect csv.Dialect `json:"dialect"`
	}{}

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		return nil, err
	}

	req := &query.ProxyRequest{}
	req.Dialect = request.Dialect

	req.Request.OrganizationID = request.OrganizationID

	switch request.Type {
	case lang.FluxCompilerType:
		req.Request.Compiler = lang.FluxCompiler{
			Query: request.Query,
		}
	case repl.CompilerType:
		req.Request.Compiler = repl.Compiler{
			Spec: request.Spec,
		}
	case influxql.CompilerType:
		req.Request.Compiler = &influxql.Compiler{
			Cluster: request.Cluster,
			DB:      request.DB,
			RP:      request.RP,
			Query:   request.Query,
		}
	default:
		return nil, fmt.Errorf("compiler type not supported")
	}

	return req, nil
}

// handlePostSourceQuery is the HTTP handler for POST /api/v2/sources/:id/query
func (h *SourceHandler) handlePostSourceQuery(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	gsr, err := decodeGetSourceRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	req, err := decodeSourceQueryRequest(r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	s, err := h.SourceService.FindSourceByID(ctx, gsr.SourceID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	querySvc, err := h.NewQueryService(s)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	_, err = querySvc.Query(ctx, w, req)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
}

// handleGetSourcesBuckets is the HTTP handler for the GET /api/v2/sources/:id/buckets route.
func (h *SourceHandler) handleGetSourcesBuckets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodeGetSourceBucketsRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	_, err = h.SourceService.FindSourceByID(ctx, req.getSourceRequest.SourceID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	bs, _, err := h.BucketService.FindBuckets(ctx, req.getBucketsRequest.filter)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusOK, newBucketsResponse(ctx, req.opts, req.filter, bs, h.LabelService)); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getSourceBucketsRequest struct {
	*getSourceRequest
	*getBucketsRequest
}

func decodeGetSourceBucketsRequest(ctx context.Context, r *http.Request) (*getSourceBucketsRequest, error) {
	getSrcReq, err := decodeGetSourceRequest(ctx, r)
	if err != nil {
		return nil, err
	}
	getBucketsReq, err := decodeGetBucketsRequest(r)
	if err != nil {
		return nil, err
	}
	return &getSourceBucketsRequest{
		getBucketsRequest: getBucketsReq,
		getSourceRequest:  getSrcReq,
	}, nil
}

// handlePostSource is the HTTP handler for the POST /api/v2/sources route.
func (h *SourceHandler) handlePostSource(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePostSourceRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.SourceService.CreateSource(ctx, req.Source); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	res := newSourceResponse(req.Source)
	h.log.Debug("Source created", zap.String("source", fmt.Sprint(res)))
	if err := encodeResponse(ctx, w, http.StatusCreated, res); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type postSourceRequest struct {
	Source *platform.Source
}

func decodePostSourceRequest(ctx context.Context, r *http.Request) (*postSourceRequest, error) {
	b := &platform.Source{}
	if err := json.NewDecoder(r.Body).Decode(b); err != nil {
		return nil, err
	}

	return &postSourceRequest{
		Source: b,
	}, nil
}

// handleGetSource is the HTTP handler for the GET /api/v2/sources/:id route.
func (h *SourceHandler) handleGetSource(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetSourceRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	s, err := h.SourceService.FindSourceByID(ctx, req.SourceID)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	res := newSourceResponse(s)
	h.log.Debug("Source retrieved", zap.String("source", fmt.Sprint(res)))

	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// handleGetSourceHealth is the HTTP handler for the GET /v1/sources/:id/health route.
func (h *SourceHandler) handleGetSourceHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	msg := `{"name":"sources","message":"source is %shealthy","status":"%s","checks":[]}`
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	req, err := decodeGetSourceRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	if _, err := h.SourceService.FindSourceByID(ctx, req.SourceID); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	// todo(leodido) > check source is actually healthy and reply with 503 if not
	// w.WriteHeader(http.StatusServiceUnavailable)
	// fmt.Fprintln(w, fmt.Sprintf(msg, "not ", "fail"))

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, fmt.Sprintf(msg, "", "pass"))
}

type getSourceRequest struct {
	SourceID platform.ID
}

func decodeGetSourceRequest(ctx context.Context, r *http.Request) (*getSourceRequest, error) {
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
	req := &getSourceRequest{
		SourceID: i,
	}

	return req, nil
}

// handleDeleteSource is the HTTP handler for the DELETE /api/v2/sources/:id route.
func (h *SourceHandler) handleDeleteSource(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeDeleteSourceRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	if err := h.SourceService.DeleteSource(ctx, req.SourceID); err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Source deleted", zap.String("sourceID", fmt.Sprint(req.SourceID)))

	w.WriteHeader(http.StatusNoContent)
}

type deleteSourceRequest struct {
	SourceID platform.ID
}

func decodeDeleteSourceRequest(ctx context.Context, r *http.Request) (*deleteSourceRequest, error) {
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
	req := &deleteSourceRequest{
		SourceID: i,
	}

	return req, nil
}

// handleGetSources is the HTTP handler for the GET /api/v2/sources route.
func (h *SourceHandler) handleGetSources(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodeGetSourcesRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	srcs, _, err := h.SourceService.FindSources(ctx, req.findOptions)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	res := newSourcesResponse(srcs)
	h.log.Debug("Sources retrieved", zap.String("sources", fmt.Sprint(res)))

	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type getSourcesRequest struct {
	findOptions platform.FindOptions
}

func decodeGetSourcesRequest(ctx context.Context, r *http.Request) (*getSourcesRequest, error) {
	req := &getSourcesRequest{}
	return req, nil
}

// handlePatchSource is the HTTP handler for the PATH /api/v2/sources route.
func (h *SourceHandler) handlePatchSource(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req, err := decodePatchSourceRequest(ctx, r)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	b, err := h.SourceService.UpdateSource(ctx, req.SourceID, req.Update)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	h.log.Debug("Source updated", zap.String("source", fmt.Sprint(b)))

	if err := encodeResponse(ctx, w, http.StatusOK, b); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

type patchSourceRequest struct {
	Update   platform.SourceUpdate
	SourceID platform.ID
}

func decodePatchSourceRequest(ctx context.Context, r *http.Request) (*patchSourceRequest, error) {
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

	var upd platform.SourceUpdate
	if err := json.NewDecoder(r.Body).Decode(&upd); err != nil {
		return nil, err
	}

	return &patchSourceRequest{
		Update:   upd,
		SourceID: i,
	}, nil
}

// SourceService connects to Influx via HTTP using tokens to manage sources
type SourceService struct {
	Client *httpc.Client
}

// FindSourceByID returns a single source by ID.
func (s *SourceService) FindSourceByID(ctx context.Context, id platform.ID) (*platform.Source, error) {
	var b platform.Source
	err := s.Client.
		Get(prefixSources, id.String()).
		DecodeJSON(&b).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// FindSources returns a list of sources that match filter and the total count of matching sources.
// Additional options provide pagination & sorting.
func (s *SourceService) FindSources(ctx context.Context, opt platform.FindOptions) ([]*platform.Source, int, error) {
	var bs []*platform.Source
	err := s.Client.
		Get(prefixSources).
		DecodeJSON(&bs).
		Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	return bs, len(bs), nil
}

// CreateSource creates a new source and sets b.ID with the new identifier.
func (s *SourceService) CreateSource(ctx context.Context, b *platform.Source) error {
	return s.Client.
		PostJSON(b, prefixSources).
		DecodeJSON(b).
		Do(ctx)
}

// UpdateSource updates a single source with changeset.
// Returns the new source state after update.
func (s *SourceService) UpdateSource(ctx context.Context, id platform.ID, upd platform.SourceUpdate) (*platform.Source, error) {
	var b platform.Source
	err := s.Client.
		PatchJSON(upd, prefixSources, id.String()).
		DecodeJSON(&b).
		Do(ctx)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// DeleteSource removes a source by ID.
func (s *SourceService) DeleteSource(ctx context.Context, id platform.ID) error {
	return s.Client.
		Delete(prefixSources, id.String()).
		StatusFn(func(resp *http.Response) error {
			return CheckErrorStatus(http.StatusNoContent, resp)
		}).
		Do(ctx)

}
