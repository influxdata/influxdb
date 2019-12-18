package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/complete"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/iocounter"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb"
	pcontext "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/http/metric"
	"github.com/influxdata/influxdb/kit/check"
	"github.com/influxdata/influxdb/kit/tracing"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/query"
	"github.com/pkg/errors"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	prefixQuery   = "/api/v2/query"
	traceIDHeader = "Trace-Id"
)

// FluxBackend is all services and associated parameters required to construct
// the FluxHandler.
type FluxBackend struct {
	influxdb.HTTPErrorHandler
	log                *zap.Logger
	QueryEventRecorder metric.EventRecorder

	OrganizationService influxdb.OrganizationService
	ProxyQueryService   query.ProxyQueryService
}

// NewFluxBackend returns a new instance of FluxBackend.
func NewFluxBackend(log *zap.Logger, b *APIBackend) *FluxBackend {
	return &FluxBackend{
		HTTPErrorHandler:   b.HTTPErrorHandler,
		log:                log,
		QueryEventRecorder: b.QueryEventRecorder,

		ProxyQueryService:   b.FluxService,
		OrganizationService: b.OrganizationService,
	}
}

// HTTPDialect is an encoding dialect that can write metadata to HTTP headers
type HTTPDialect interface {
	SetHeaders(w http.ResponseWriter)
}

// FluxHandler implements handling flux queries.
type FluxHandler struct {
	*httprouter.Router
	influxdb.HTTPErrorHandler
	log *zap.Logger

	Now                 func() time.Time
	OrganizationService influxdb.OrganizationService
	ProxyQueryService   query.ProxyQueryService

	EventRecorder metric.EventRecorder
}

// Prefix provides the route prefix.
func (*FluxHandler) Prefix() string {
	return prefixQuery
}

// NewFluxHandler returns a new handler at /api/v2/query for flux queries.
func NewFluxHandler(log *zap.Logger, b *FluxBackend) *FluxHandler {
	h := &FluxHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		Now:              time.Now,
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,

		ProxyQueryService:   b.ProxyQueryService,
		OrganizationService: b.OrganizationService,
		EventRecorder:       b.QueryEventRecorder,
	}

	// query reponses can optionally be gzip encoded
	qh := gziphandler.GzipHandler(http.HandlerFunc(h.handleQuery))
	h.Handler("POST", prefixQuery, qh)
	h.HandlerFunc("POST", "/api/v2/query/ast", h.postFluxAST)
	h.HandlerFunc("POST", "/api/v2/query/analyze", h.postQueryAnalyze)
	h.HandlerFunc("GET", "/api/v2/query/suggestions", h.getFluxSuggestions)
	h.HandlerFunc("GET", "/api/v2/query/suggestions/:name", h.getFluxSuggestion)
	return h
}

func (h *FluxHandler) handleQuery(w http.ResponseWriter, r *http.Request) {
	const op = "http/handlePostQuery"
	span, r := tracing.ExtractFromHTTPRequest(r, "FluxHandler")
	defer span.Finish()

	ctx := r.Context()
	log := h.log.With(logger.TraceFields(ctx)...)
	if id, _, found := tracing.InfoFromContext(ctx); found {
		w.Header().Set(traceIDHeader, id)
	}

	// TODO(desa): I really don't like how we're recording the usage metrics here
	// Ideally this will be moved when we solve https://github.com/influxdata/influxdb/issues/13403
	var orgID influxdb.ID
	var requestBytes int
	sw := newStatusResponseWriter(w)
	w = sw
	defer func() {
		h.EventRecorder.Record(ctx, metric.Event{
			OrgID:         orgID,
			Endpoint:      r.URL.Path, // This should be sufficient for the time being as it should only be single endpoint.
			RequestBytes:  requestBytes,
			ResponseBytes: sw.responseBytes,
			Status:        sw.code(),
		})
	}()

	a, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		err := &influxdb.Error{
			Code: influxdb.EUnauthorized,
			Msg:  "authorization is invalid or missing in the query request",
			Op:   op,
			Err:  err,
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}

	req, n, err := decodeProxyQueryRequest(ctx, r, a, h.OrganizationService)
	if err != nil && err != influxdb.ErrAuthorizerNotSupported {
		err := &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "failed to decode request body",
			Op:   op,
			Err:  err,
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	orgID = req.Request.OrganizationID
	requestBytes = n

	// Transform the context into one with the request's authorization.
	ctx = pcontext.SetAuthorizer(ctx, req.Request.Authorization)

	hd, ok := req.Dialect.(HTTPDialect)
	if !ok {
		err := &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("unsupported dialect over HTTP: %T", req.Dialect),
			Op:   op,
		}
		h.HandleHTTPError(ctx, err, w)
		return
	}
	hd.SetHeaders(w)

	cw := iocounter.Writer{Writer: w}
	if _, err := h.ProxyQueryService.Query(ctx, &cw, req); err != nil {
		if cw.Count() == 0 {
			// Only record the error headers IFF nothing has been written to w.
			h.HandleHTTPError(ctx, err, w)
			return
		}
		_ = tracing.LogError(span, err)
		log.Info("Error writing response to client",
			zap.String("handler", "flux"),
			zap.Error(err),
		)
	}
}

type langRequest struct {
	Query string `json:"query"`
}

type postFluxASTResponse struct {
	AST *ast.Package `json:"ast"`
}

// postFluxAST returns a flux AST for provided flux string
func (h *FluxHandler) postFluxAST(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "FluxHandler")
	defer span.Finish()

	var request langRequest
	ctx := r.Context()

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		h.HandleHTTPError(ctx, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid json",
			Err:  err,
		}, w)
		return
	}

	pkg := parser.ParseSource(request.Query)
	if ast.Check(pkg) > 0 {
		err := ast.GetError(pkg)
		h.HandleHTTPError(ctx, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid AST",
			Err:  err,
		}, w)
		return
	}

	res := postFluxASTResponse{
		AST: pkg,
	}

	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// postQueryAnalyze parses a query and returns any query errors.
func (h *FluxHandler) postQueryAnalyze(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "FluxHandler")
	defer span.Finish()

	ctx := r.Context()

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.HandleHTTPError(ctx, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid json",
			Err:  err,
		}, w)
		return
	}

	a, err := req.Analyze()
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusOK, a); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// fluxParams contain flux funciton parameters as defined by the semantic graph
type fluxParams map[string]string

// suggestionResponse provides the parameters available for a given Flux function
type suggestionResponse struct {
	Name   string     `json:"name"`
	Params fluxParams `json:"params"`
}

// suggestionsResponse provides a list of available Flux functions
type suggestionsResponse struct {
	Functions []suggestionResponse `json:"funcs"`
}

// getFluxSuggestions returns a list of available Flux functions for the Flux Builder
func (h *FluxHandler) getFluxSuggestions(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "FluxHandler")
	defer span.Finish()

	ctx := r.Context()
	completer := complete.DefaultCompleter()
	names := completer.FunctionNames()
	var functions []suggestionResponse
	for _, name := range names {
		suggestion, err := completer.FunctionSuggestion(name)
		if err != nil {
			h.HandleHTTPError(ctx, err, w)
			return
		}

		filteredParams := make(fluxParams)
		for key, value := range suggestion.Params {
			if key == "table" {
				continue
			}

			filteredParams[key] = value
		}

		functions = append(functions, suggestionResponse{
			Name:   name,
			Params: filteredParams,
		})
	}
	res := suggestionsResponse{Functions: functions}

	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// getFluxSuggestion returns the function parameters for the requested function
func (h *FluxHandler) getFluxSuggestion(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "FluxHandler")
	defer span.Finish()

	ctx := r.Context()
	name := httprouter.ParamsFromContext(ctx).ByName("name")
	completer := complete.DefaultCompleter()

	suggestion, err := completer.FunctionSuggestion(name)
	if err != nil {
		h.HandleHTTPError(ctx, err, w)
		return
	}

	res := suggestionResponse{Name: name, Params: suggestion.Params}
	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		logEncodingError(h.log, r, err)
		return
	}
}

// PrometheusCollectors satisifies the prom.PrometheusCollector interface.
func (h *FluxHandler) PrometheusCollectors() []prom.Collector {
	// TODO: gather and return relevant metrics.
	return nil
}

var _ query.ProxyQueryService = (*FluxService)(nil)

// FluxService connects to Influx via HTTP using tokens to run queries.
type FluxService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// Query runs a flux query against a influx server and sends the results to the io.Writer.
// Will use the token from the context over the token within the service struct.
func (s *FluxService) Query(ctx context.Context, w io.Writer, r *query.ProxyRequest) (flux.Statistics, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	u, err := NewURL(s.Addr, prefixQuery)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	params := url.Values{}
	params.Set(OrgID, r.Request.OrganizationID.String())
	u.RawQuery = params.Encode()

	qreq, err := QueryRequestFromProxyRequest(r)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qreq); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	SetToken(s.Token, hreq)

	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/csv")
	hreq = hreq.WithContext(ctx)

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	if _, err := io.Copy(w, resp.Body); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	return flux.Statistics{}, nil
}

func (s FluxService) Check(ctx context.Context) check.Response {
	return QueryHealthCheck(s.Addr, s.InsecureSkipVerify)
}

var _ query.QueryService = (*FluxQueryService)(nil)

// FluxQueryService implements query.QueryService by making HTTP requests to the /api/v2/query API endpoint.
type FluxQueryService struct {
	Addr               string
	Token              string
	InsecureSkipVerify bool
}

// Query runs a flux query against a influx server and decodes the result
func (s *FluxQueryService) Query(ctx context.Context, r *query.Request) (flux.ResultIterator, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(s.Addr, prefixQuery)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	params := url.Values{}
	params.Set(OrgID, r.OrganizationID.String())
	u.RawQuery = params.Encode()

	preq := &query.ProxyRequest{
		Request: *r,
		Dialect: csv.DefaultDialect(),
	}
	qreq, err := QueryRequestFromProxyRequest(preq)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qreq); err != nil {
		return nil, tracing.LogError(span, err)
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	SetToken(s.Token, hreq)

	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/csv")
	hreq = hreq.WithContext(ctx)

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	// Can't defer resp.Body.Close here because the CSV decoder depends on reading from resp.Body after this function returns.

	if err := CheckError(resp); err != nil {
		return nil, tracing.LogError(span, err)
	}

	decoder := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
	itr, err := decoder.Decode(resp.Body)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	return itr, nil
}

func (s FluxQueryService) Check(ctx context.Context) check.Response {
	return QueryHealthCheck(s.Addr, s.InsecureSkipVerify)
}

// SimpleQuery runs a flux query with common parameters and returns CSV results.
func SimpleQuery(addr, flux, org, token string) ([]byte, error) {
	u, err := NewURL(addr, prefixQuery)
	if err != nil {
		return nil, err
	}
	params := url.Values{}
	params.Set(Org, org)
	u.RawQuery = params.Encode()

	header := true
	qr := &QueryRequest{
		Type:  "flux",
		Query: flux,
		Dialect: QueryDialect{
			Header:         &header,
			Delimiter:      ",",
			CommentPrefix:  "#",
			DateTimeFormat: "RFC3339",
		},
	}

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qr); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return nil, err
	}

	SetToken(token, req)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/csv")

	insecureSkipVerify := false
	hc := NewClient(u.Scheme, insecureSkipVerify)
	res, err := hc.Do(req)
	if err != nil {
		return nil, err
	}

	if err := CheckError(res); err != nil {
		return nil, err
	}

	defer res.Body.Close()
	return ioutil.ReadAll(res.Body)
}

func QueryHealthCheck(url string, insecureSkipVerify bool) check.Response {
	u, err := NewURL(url, "/health")
	if err != nil {
		return check.Response{
			Name:    "query health",
			Status:  check.StatusFail,
			Message: errors.Wrap(err, "could not form URL").Error(),
		}
	}

	hc := NewClient(u.Scheme, insecureSkipVerify)
	resp, err := hc.Get(u.String())
	if err != nil {
		return check.Response{
			Name:    "query health",
			Status:  check.StatusFail,
			Message: errors.Wrap(err, "error getting response").Error(),
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return check.Response{
			Name:    "query health",
			Status:  check.StatusFail,
			Message: fmt.Sprintf("http error %v", resp.StatusCode),
		}
	}

	var healthResponse check.Response
	if err = json.NewDecoder(resp.Body).Decode(&healthResponse); err != nil {
		return check.Response{
			Name:    "query health",
			Status:  check.StatusFail,
			Message: errors.Wrap(err, "error decoding JSON response").Error(),
		}
	}

	return healthResponse
}
