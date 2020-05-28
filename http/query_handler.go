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
	"sort"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/iocounter"
	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/http/metric"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/influxql"
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

	AlgoWProxy          FeatureProxyHandler
	OrganizationService influxdb.OrganizationService
	ProxyQueryService   query.ProxyQueryService
	FluxLanguageService influxdb.FluxLanguageService
}

// NewFluxBackend returns a new instance of FluxBackend.
func NewFluxBackend(log *zap.Logger, b *APIBackend) *FluxBackend {
	return &FluxBackend{
		HTTPErrorHandler:   b.HTTPErrorHandler,
		log:                log,
		QueryEventRecorder: b.QueryEventRecorder,
		AlgoWProxy:         b.AlgoWProxy,
		ProxyQueryService: routingQueryService{
			InfluxQLService: b.InfluxQLService,
			DefaultService:  b.FluxService,
		},
		OrganizationService: b.OrganizationService,
		FluxLanguageService: b.FluxLanguageService,
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
	FluxLanguageService influxdb.FluxLanguageService

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
		FluxLanguageService: b.FluxLanguageService,
	}

	// query reponses can optionally be gzip encoded
	qh := gziphandler.GzipHandler(http.HandlerFunc(h.handleQuery))
	h.Handler("POST", prefixQuery, withFeatureProxy(b.AlgoWProxy, qh))
	h.Handler("POST", "/api/v2/query/ast", withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.postFluxAST)))
	h.Handler("POST", "/api/v2/query/analyze", withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.postQueryAnalyze)))
	h.Handler("GET", "/api/v2/query/suggestions", withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.getFluxSuggestions)))
	h.Handler("GET", "/api/v2/query/suggestions/:name", withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.getFluxSuggestion)))
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
	sw := kithttp.NewStatusResponseWriter(w)
	w = sw
	defer func() {
		h.EventRecorder.Record(ctx, metric.Event{
			OrgID:         orgID,
			Endpoint:      r.URL.Path, // This should be sufficient for the time being as it should only be single endpoint.
			RequestBytes:  requestBytes,
			ResponseBytes: sw.ResponseBytes(),
			Status:        sw.Code(),
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
	req.Request.Source = r.Header.Get("User-Agent")
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

	pkg, err := query.Parse(h.FluxLanguageService, request.Query)
	if err != nil {
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

	a, err := req.Analyze(h.FluxLanguageService)
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
	completer := h.FluxLanguageService.Completer()
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
	completer := h.FluxLanguageService.Completer()

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
	Name               string
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
	if r.Request.Source != "" {
		hreq.Header.Add("User-Agent", r.Request.Source)
	} else if s.Name != "" {
		hreq.Header.Add("User-Agent", s.Name)
	}

	// Now that the request is all set, we can apply header mutators.
	if err := r.Request.ApplyOptions(hreq.Header); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

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
	Name               string
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
	if r.Source != "" {
		hreq.Header.Add("User-Agent", r.Source)
	} else if s.Name != "" {
		hreq.Header.Add("User-Agent", s.Name)
	}
	hreq = hreq.WithContext(ctx)

	// Now that the request is all set, we can apply header mutators.
	if err := r.ApplyOptions(hreq.Header); err != nil {
		return nil, tracing.LogError(span, err)
	}

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

// GetQueryResponse runs a flux query with common parameters and returns the response from the query service.
func GetQueryResponse(qr *QueryRequest, addr, org, token string, headers ...string) (*http.Response, error) {
	if len(headers)%2 != 0 {
		return nil, fmt.Errorf("headers must be key value pairs")
	}
	u, err := NewURL(addr, prefixQuery)
	if err != nil {
		return nil, err
	}
	params := url.Values{}
	params.Set(Org, org)
	u.RawQuery = params.Encode()

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qr); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return nil, err
	}

	SetToken(token, req)

	// Default headers.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/csv")
	// Apply custom headers.
	for i := 0; i < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}

	insecureSkipVerify := false
	hc := NewClient(u.Scheme, insecureSkipVerify)
	return hc.Do(req)
}

// GetQueryResponseBody reads the body of a response from some query service.
// It also checks for errors in the response.
func GetQueryResponseBody(res *http.Response) ([]byte, error) {
	if err := CheckError(res); err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return ioutil.ReadAll(res.Body)
}

// SimpleQuery runs a flux query with common parameters and returns CSV results.
func SimpleQuery(addr, flux, org, token string, headers ...string) ([]byte, error) {
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
	res, err := GetQueryResponse(qr, addr, org, token, headers...)
	if err != nil {
		return nil, err
	}
	return GetQueryResponseBody(res)
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

// routingQueryService routes queries to specific query services based on their compiler type.
type routingQueryService struct {
	// InfluxQLService handles queries with compiler type of "influxql"
	InfluxQLService query.ProxyQueryService
	// DefaultService handles all other queries
	DefaultService query.ProxyQueryService
}

func (s routingQueryService) Check(ctx context.Context) check.Response {
	// Produce combined check response
	response := check.Response{
		Name:   "internal-routingQueryService",
		Status: check.StatusPass,
	}
	def := s.DefaultService.Check(ctx)
	influxql := s.InfluxQLService.Check(ctx)
	if def.Status == check.StatusFail {
		response.Status = def.Status
		response.Message = def.Message
	} else if influxql.Status == check.StatusFail {
		response.Status = influxql.Status
		response.Message = influxql.Message
	}
	response.Checks = []check.Response{def, influxql}
	sort.Sort(response.Checks)
	return response
}

func (s routingQueryService) Query(ctx context.Context, w io.Writer, req *query.ProxyRequest) (flux.Statistics, error) {
	if req.Request.Compiler.CompilerType() == influxql.CompilerType {
		return s.InfluxQLService.Query(ctx, w, req)
	}
	return s.DefaultService.Query(ctx, w, req)
}
